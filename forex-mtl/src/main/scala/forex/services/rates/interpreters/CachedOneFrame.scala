package forex.services.rates.interpreters

import cats.effect.{Clock, ConcurrentEffect, Sync}
import cats.implicits.{toShow}
import cats.syntax.either._
import cats.syntax.flatMap._
import cats.syntax.functor._
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
    Sync[F].delay(logger.info(s"Requesting rate for pair: ${pair.from.show}${pair.to.show}")) >>
    cache.get(pair).flatMap {
      case Some(cachedRate) =>
        Sync[F].delay(logger.info(s"Cache HIT for ${pair.from.show}${pair.to.show}")) >>
        ConcurrentEffect[F].pure(cachedRate.asRight[Error])
      case None =>
        Sync[F].delay(logger.info(s"Cache MISS for ${pair.from.show}${pair.to.show}")) >>
        cache.getExpiredTrackedPairs.flatMap { expiredPairs =>
          if (expiredPairs.nonEmpty) {
            val expiredPairsStr = expiredPairs.map(p => s"${p.from.show}${p.to.show}").mkString(", ")
            Sync[F].delay(logger.info(s"Found expired tracked pairs: [${expiredPairsStr}]. Making batch request.")) >>
            client.asInstanceOf[OneFrameClient[F]].getBatch(expiredPairs).flatMap {
              case Right(rates) =>
                Sync[F].delay(logger.info(s"Batch API call successful. Received ${rates.length} rates. Caching all.")) >>
                cache.putBatch(rates).flatMap { _ =>
                  rates.find(_.pair == pair) match {
                    case Some(rate) => 
                      Sync[F].delay(logger.info(s"Returning requested rate for ${pair.from.show}${pair.to.show}: ${rate.price.value}")) >>
                      ConcurrentEffect[F].pure(rate.asRight[Error])
                    case None => 
                      Sync[F].delay(logger.info(s"ERROR: Requested pair ${pair.from.show}${pair.to.show} not found in batch response")) >>
                      ConcurrentEffect[F].pure(OneFrameLookupFailed("Pair not found in response").asLeft[Rate])
                  }
                }
              case Left(error) =>
                Sync[F].delay(logger.info(s"Batch API call failed: ${error}")) >>
                ConcurrentEffect[F].pure(error.asLeft[Rate])
            }
          } else {
            Sync[F].delay(logger.info(s"No other expired pairs. Making single request for ${pair.from.show}${pair.to.show}")) >>
            client.get(pair).flatMap {
              case Right(rate) =>
                Sync[F].delay(logger.info(s"Single API call successful. Caching rate for ${pair.from.show}${pair.to.show}: ${rate.price.value}")) >>
                cache.put(rate).map(_ => rate.asRight[Error])
              case Left(error) =>
                Sync[F].delay(logger.info(s"Single API call failed for ${pair.from.show}${pair.to.show}: ${error}")) >>
                ConcurrentEffect[F].pure(error.asLeft[Rate])
            }
          }
        }
    }
  }
}

object CachedOneFrame {
  def apply[F[_]: ConcurrentEffect: Clock](implicit ec: ExecutionContext): CachedOneFrame[F] = {
    val client = new OneFrameClient[F]()
    val cache = new RateCache[F]()
    new CachedOneFrame[F](client, cache)
  }
}