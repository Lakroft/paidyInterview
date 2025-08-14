package forex.services.rates.interpreters

import cats.effect.{Clock, ConcurrentEffect}
import cats.implicits.toShow
import cats.syntax.either._
import cats.syntax.flatMap._
import forex.config.{CacheConfig, OneFrameConfig}
import forex.domain.Rate
import forex.services.rates.errors.Error.OneFrameLookupFailed
import forex.services.rates.{Algebra, RateCache}
import forex.services.rates.errors._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

class CachedOneFrame[F[_]: ConcurrentEffect](
    client: Algebra[F],
    cache: RateCache[F]
) extends Algebra[F] {

  private val logger = LoggerFactory.getLogger(classOf[CachedOneFrame[F]])

  override def get(pair: Rate.Pair): F[Error Either Rate] = {
    cache.get(pair).flatMap {
      case Some(cachedRate) =>
        logger.debug(s"Cache HIT for ${pair.from.show}${pair.to.show}")
        ConcurrentEffect[F].pure(cachedRate.asRight[Error])
      case None =>
        logger.debug(s"Cache MISS for ${pair.from.show}${pair.to.show}")
        cache.getExpiredTrackedPairs.flatMap { expiredPairs =>
          val pairsToFetch = (expiredPairs :+ pair).distinct // here could be duplication but this way we can see how it's working
          val pairsStr = pairsToFetch.map(p => s"${p.from.show}${p.to.show}").mkString(", ")
          logger.info(s"Batch request for pairs: [${pairsStr}]")
          
          client.getBatch(pairsToFetch).flatMap {
            case Right(rates) =>
              cache.putBatch(rates).flatMap { _ =>
                rates.find(_.pair == pair) match {
                  case Some(rate) => 
                    ConcurrentEffect[F].pure(rate.asRight[Error])
                  case None => 
                    logger.warn(s"Requested pair ${pair.from.show}${pair.to.show} not found in batch response")
                    ConcurrentEffect[F].pure(OneFrameLookupFailed("Pair not found in response").asLeft[Rate])
                }
              }
            case Left(error) =>
              logger.error(s"Batch API call failed: ${error}")
              ConcurrentEffect[F].pure(error.asLeft[Rate])
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