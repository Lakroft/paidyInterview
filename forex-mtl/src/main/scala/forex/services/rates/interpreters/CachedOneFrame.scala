package forex.services.rates.interpreters

import cats.effect.concurrent.Ref
import cats.effect.{Clock, ConcurrentEffect}
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.apply._
import forex.config.{CacheConfig, OneFrameConfig}
import forex.domain.{Currency, Rate}
import forex.services.rates.errors.Error.{InvalidCurrencyPair, RateNotFound}
import forex.services.rates.{Algebra, RateCache}
import forex.services.rates.errors._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

class CachedOneFrame[F[_]: ConcurrentEffect](
    client: Algebra[F],
    cache: RateCache[F],
    loadingRef: Ref[F, Boolean]
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
    cache.get(pair).flatMap {
      case Some(cachedRate) =>
        logger.debug(s"Cache HIT for ${pair.from}${pair.to} (unsynchronized read)")
        ConcurrentEffect[F].pure(cachedRate.asRight[Error])
      case None =>
        loadingRef.get.flatMap { isLoading =>
          if (isLoading) {
            // Another thread is loading - wait and then check cache again
            logger.debug(s"Another thread is loading cache for ${pair.from}${pair.to} - waiting")
            ConcurrentEffect[F].delay(Thread.sleep(10)) *> // Small delay
            cache.get(pair).flatMap {
              case Some(cachedRate) =>
                logger.debug(s"Cache HIT for ${pair.from}${pair.to} (after waiting)")
                ConcurrentEffect[F].pure(cachedRate.asRight[Error])
              case None =>
                // Still no cache, try again (with reasonable limit)
                getCurrencyRate(pair)
            }
          } else {
            // Try to acquire loading lock
            loadingRef.modify { current =>
              if (current) {
                // Someone else is already loading
                (current, false) // Don't change state, return false
              } else {
                // Acquire the lock
                (true, true) // Set loading to true, return true
              }
            }.flatMap { acquired =>
              if (acquired) {
                // We acquired the lock - double check cache and load if needed
                cache.get(pair).flatMap {
                  case Some(cachedRate) =>
                    // Cache appeared while we were acquiring lock
                    loadingRef.set(false).map(_ => cachedRate.asRight[Error])
                  case None =>
                    logger.debug(s"Cache MISS for ${pair.from}${pair.to} - performing batch API call")
                    performBatchAPICall(pair).flatTap(_ => loadingRef.set(false))
                }
              } else {
                // Someone else got the lock, wait and retry
                logger.debug(s"Failed to acquire lock for ${pair.from}${pair.to} - retrying")
                getCurrencyRate(pair)
              }
            }
          }
        }
    }
  }

  private def performBatchAPICall(pair: Rate.Pair): F[Error Either Rate] = {
    val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }
    val pairsStr = allSupportedPairs.map(p => s"${p.from}${p.to}").mkString(", ")
    logger.info(s"Batch request for ALL supported pairs: [$pairsStr]")
    
    client.getBatch(allSupportedPairs).flatMap {
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

object CachedOneFrame {
  def apply[F[_]: ConcurrentEffect: Clock](
      oneFrameConfig: OneFrameConfig, 
      cacheConfig: CacheConfig
  )(implicit ec: ExecutionContext): F[CachedOneFrame[F]] = {
    for {
      loadingRef <- Ref.of[F, Boolean](false)
      client = new OneFrameClient[F](oneFrameConfig)
      cache = new RateCache[F](cacheConfig)
    } yield new CachedOneFrame[F](client, cache, loadingRef)
  }
}