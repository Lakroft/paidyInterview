package forex.services.rates

import cats.effect.{Clock, Sync}
import cats.syntax.functor._
import forex.config.CacheConfig
import forex.domain.Rate

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.jdk.CollectionConverters._

case class CachedRate(rate: Rate, expiresAt: Instant)

class RateCache[F[_]: Sync: Clock](config: CacheConfig) {
  
  private val cache = new ConcurrentHashMap[Rate.Pair, CachedRate]()
  private val trackedPairs = ConcurrentHashMap.newKeySet[Rate.Pair]()
  private val ttl = config.ttl

  def get(pair: Rate.Pair): F[Option[Rate]] = {
    trackedPairs.add(pair)
    
    Clock[F].realTime(MILLISECONDS).map { nowMillis =>
      Option(cache.get(pair)).flatMap { cachedRate =>
        if (cachedRate.expiresAt.isAfter(Instant.ofEpochMilli(nowMillis))) {
          Some(cachedRate.rate)
        } else {
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
  
  def getTrackedPairs: F[List[Rate.Pair]] = {
    Sync[F].delay(trackedPairs.asScala.toList)
  }
  
  def getExpiredTrackedPairs: F[List[Rate.Pair]] = {
    Clock[F].realTime(MILLISECONDS).map { nowMillis =>
      val now = Instant.ofEpochMilli(nowMillis)
      trackedPairs.asScala.toList.filter { pair =>
        Option(cache.get(pair)) match {
          case Some(cachedRate) => cachedRate.expiresAt.isBefore(now) || cachedRate.expiresAt.equals(now)
          case None => true
        }
      }
    }
  }
  
  def putBatch(rates: List[Rate]): F[Unit] = {
    Sync[F].delay {
      rates.foreach { rate =>
        val apiTimestamp = rate.timestamp.value.toInstant
        val expiresAt = apiTimestamp.plusSeconds(ttl.toSeconds)
        cache.put(rate.pair, CachedRate(rate, expiresAt))
      }
    }
  }
}