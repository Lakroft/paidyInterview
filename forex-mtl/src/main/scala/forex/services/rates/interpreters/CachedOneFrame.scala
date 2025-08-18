package forex.services.rates.interpreters

import cats.effect.{Clock, ConcurrentEffect}
import cats.syntax.either._
import cats.syntax.flatMap._
import forex.config.{CacheConfig, OneFrameConfig}
import forex.domain.Rate
import forex.services.rates.errors.Error.{InvalidCurrencyPair, RateNotFound}
import forex.services.rates.{Algebra, RateCache}
import forex.services.rates.errors._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

class CachedOneFrame[F[_]: ConcurrentEffect](
    client: Algebra[F],
    cache: RateCache[F]
) extends Algebra[F] {

  private val logger = LoggerFactory.getLogger(classOf[CachedOneFrame[F]])

  private def validateCurrencyPair(pair: Rate.Pair): Either[Error, Rate.Pair] = {
    val pairStr = s"${pair.from}${pair.to}"
    
    if (pair.from == pair.to) {
      Left(InvalidCurrencyPair(pairStr, "same currency conversion not supported"))
    } else {
      Right(pair)
    }
  }

  override def get(pair: Rate.Pair): F[Error Either Rate] = {
    validateCurrencyPair(pair) match {
      case Left(error) =>
        logger.warn(s"Invalid currency pair validation failed: ${error.message}")
        ConcurrentEffect[F].pure(error.asLeft[Rate])
      case Right(validPair) =>
        getCurrencyRate(validPair)
    }
  }

  private def getCurrencyRate(pair: Rate.Pair): F[Error Either Rate] = {
    this.synchronized {
      cache.get(pair).flatMap {
        case Some(cachedRate) =>
          logger.debug(s"Cache HIT for ${pair.from}${pair.to}")
          ConcurrentEffect[F].pure(cachedRate.asRight[Error])
        case None =>
          logger.debug(s"Cache MISS for ${pair.from}${pair.to}")
          cache.getAllCachedPairs.flatMap { allCachedPairs =>
            val pairsToFetch = (allCachedPairs :+ pair).distinct
            val pairsStr = pairsToFetch.map(p => s"${p.from}${p.to}").mkString(", ")
            logger.info(s"Batch request for ALL pairs: [$pairsStr]")
            
            client.getBatch(pairsToFetch).flatMap {
              case Right(rates) =>
                cache.putBatch(rates).flatMap { _ =>
                  rates.find(_.pair == pair) match {
                    case Some(rate) => 
                      ConcurrentEffect[F].pure(rate.asRight[Error])
                    case None => 
                      val pairStr = s"${pair.from}${pair.to}"
                      logger.warn(s"Requested pair $pairStr not found in batch response")
                      ConcurrentEffect[F].pure(RateNotFound(pairStr).asLeft[Rate])
                  }
                }
              case Left(error) =>
                logger.error(s"Batch API call failed: $error")
                ConcurrentEffect[F].pure(error.asLeft[Rate])
            }
          }
      }
    }
  }

}

object CachedOneFrame {
  def apply[F[_]: ConcurrentEffect: Clock](
      oneFrameConfig: OneFrameConfig, 
      cacheConfig: CacheConfig
  )(implicit ec: ExecutionContext): CachedOneFrame[F] = {
    val client = new OneFrameClient[F](oneFrameConfig)
    val cache = new RateCache[F](cacheConfig)
    new CachedOneFrame[F](client, cache)
  }
}