package forex.services.rates.interpreters

import cats.effect.{ConcurrentEffect, Resource}
import cats.implicits._
import forex.config.{OneFrameConfig}
import forex.domain.{Currency, Rate}
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
        ConcurrentEffect[F].delay(logger.warn(s"Invalid currency pair validation failed: ${error.message}")).flatMap { _ =>
          ConcurrentEffect[F].pure(error.asLeft[Rate])
        }
      case Right(validPair) =>
        getCurrencyRate(validPair)
    }
  }

  private def getCurrencyRate(pair: Rate.Pair): F[Error Either Rate] = {
    cache.get(pair).flatMap {
      case Some(cachedRate) =>
        ConcurrentEffect[F].delay(logger.debug(s"Cache HIT for ${pair.from}${pair.to}")).as(cachedRate.asRight[Error])
      case None =>
        val pairStr = s"${pair.from}${pair.to}"
        ConcurrentEffect[F].delay(logger.debug(s"Cache MISS for $pairStr - data will be available after timer update")).as((RateNotFound(pairStr): Error).asLeft[Rate])
    }
  }


  override def getBatch(pairs: List[Rate.Pair]): F[Error Either List[Rate]] = {
    if (pairs.isEmpty) {
      ConcurrentEffect[F].pure(List.empty[Rate].asRight[Error])
    } else {
      // Validate all pairs first
      val validationResults = pairs.map(validateCurrencyPair)
      val errors = validationResults.collect { case Left(error) => error }
      
      if (errors.nonEmpty) {
        // Return first validation error
        ConcurrentEffect[F].pure(errors.head.asLeft[List[Rate]])
      } else {
        val validatedPairs = validationResults.collect { case Right(pair) => pair }
        
        // Get rates for all valid pairs using sequence to collect results
        validatedPairs.traverse(getCurrencyRate).map { results =>
          // Convert List[Either[Error, Rate]] to Either[Error, List[Rate]]
          results.sequence
        }
      }
    }
  }

  // Method to refresh cache by fetching fresh data from API and atomically replacing cache
  def refreshCache(): F[Error Either List[Rate]] = {
    val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }
    ConcurrentEffect[F].delay(logger.info(s"Refreshing cache with fresh data for ${allSupportedPairs.length} pairs")).flatMap { _ =>
      client.getBatch(allSupportedPairs).flatMap {
        case Right(rates) =>
          cache.replaceCache(rates).map(_ => rates.asRight[Error])
        case Left(error) =>
          ConcurrentEffect[F].delay(logger.error(s"Failed to refresh cache: ${error.message}")).flatMap { _ =>
            ConcurrentEffect[F].pure(error.asLeft[List[Rate]])
          }
      }
    }
  }

}

object CachedOneFrame {
  def apply[F[_]: ConcurrentEffect](
      oneFrameConfig: OneFrameConfig
  )(implicit ec: ExecutionContext): Resource[F, CachedOneFrame[F]] = {
    OneFrameClient[F](oneFrameConfig).map { client =>
      val cache = new RateCache[F]()
      new CachedOneFrame[F](client, cache)
    }
  }
}