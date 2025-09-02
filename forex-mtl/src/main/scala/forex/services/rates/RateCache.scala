package forex.services.rates

import cats.effect.{Clock, Sync}
import cats.implicits.toFlatMapOps
import cats.syntax.functor._
import forex.config.CacheConfig
import forex.domain.Rate
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.collection.concurrent.TrieMap


class RateCache[F[_]: Sync: Clock](config: CacheConfig) {
  
  private val cache = TrieMap[Rate.Pair, Rate]()
  @volatile private var cacheExpiresAt: Option[Instant] = None
  private val ttl = config.ttl
  private val logger = LoggerFactory.getLogger(classOf[RateCache[F]])

  def get(pair: Rate.Pair): F[Option[Rate]] = {
    Clock[F].realTime(MILLISECONDS).map { nowMillis =>
      val now = Instant.ofEpochMilli(nowMillis)
      cacheExpiresAt match {
        case Some(expiresAt) if expiresAt.isAfter(now) =>
          cache.get(pair) match {
            case Some(rate) =>
              logger.info(s"Cache HIT for ${pair.from}${pair.to}")
              Some(rate)
            case None =>
              None
          }
        case Some(expiresAt) =>
          logger.info(s"Cache OUTDATED for all pairs. Now: $now, expires at: $expiresAt")
          cache.clear()
          cacheExpiresAt = None
          None
        case None =>
          None
      }
    }
  }

  def put(rate: Rate): F[Unit] = {
    Clock[F].realTime(MILLISECONDS).flatMap { nowMillis =>
      Sync[F].delay {
        val expiresAt = Instant.ofEpochMilli(nowMillis).plusMillis(ttl.toMillis)
        cache.put(rate.pair, rate)
        cacheExpiresAt = Some(expiresAt)
      }
    }
  }

  def clear(): F[Unit] = Sync[F].delay {
    cache.clear()
    cacheExpiresAt = None
  }
  
  def getAllCachedPairs: F[List[Rate.Pair]] = {
    Sync[F].delay(cache.keys.toList)
  }
  
  def putBatch(rates: List[Rate]): F[Unit] = {
    Clock[F].realTime(MILLISECONDS).flatMap { nowMillis =>
      Sync[F].delay {
        val expiresAt = Instant.ofEpochMilli(nowMillis).plusMillis(ttl.toMillis)
        cache.clear()
        rates.foreach { rate =>
          cache.put(rate.pair, rate)
        }
        cacheExpiresAt = Some(expiresAt)
      }
    }
  }
}