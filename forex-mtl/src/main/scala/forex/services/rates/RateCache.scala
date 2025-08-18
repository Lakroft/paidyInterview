package forex.services.rates

import cats.effect.{Clock, Sync}
import cats.syntax.functor._
import forex.config.CacheConfig
import forex.domain.Rate
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.collection.concurrent.TrieMap

final case class CachedRate(rate: Rate, expiresAt: Instant)

class RateCache[F[_]: Sync: Clock](config: CacheConfig) {
  
  private val cache = TrieMap[Rate.Pair, CachedRate]()
  private val ttl = config.ttl
  private val logger = LoggerFactory.getLogger(classOf[RateCache[F]])

  def get(pair: Rate.Pair): F[Option[Rate]] = {
    Clock[F].realTime(MILLISECONDS).map { nowMillis =>
      cache.get(pair).flatMap { cachedRate =>
        if (cachedRate.expiresAt.isAfter(Instant.ofEpochMilli(nowMillis))) {
          logger.info(s"Cache HIT for ${pair.from}${pair.to}")
          Some(cachedRate.rate)
        } else {
          logger.info(s"Cache OUTDATED for ${pair.from}${pair.to}. Now: ${Instant.ofEpochMilli(nowMillis)}, expires at: ${cachedRate.expiresAt}")
          cache.remove(pair)
          None
        }
      }
    }
  }

  def put(rate: Rate): F[Unit] = {
    putBatch(List(rate))
  }

  def clear(): F[Unit] = Sync[F].delay(cache.clear())
  
  def getAllCachedPairs: F[List[Rate.Pair]] = {
    Sync[F].delay(cache.keys.toList)
  }
  
  def putBatch(rates: List[Rate]): F[Unit] = {
    Sync[F].delay {
      rates.foreach { rate =>
        val apiTimestamp = rate.timestamp.value.toInstant
        val expiresAt = apiTimestamp.plusMillis(ttl.toMillis)
        cache.put(rate.pair, CachedRate(rate, expiresAt))
      }
    }
  }
}