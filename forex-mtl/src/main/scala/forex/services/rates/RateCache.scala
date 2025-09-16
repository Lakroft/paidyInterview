package forex.services.rates

import cats.effect.Sync
import cats.implicits._
import forex.domain.Rate
import org.slf4j.LoggerFactory
import scala.collection.concurrent.TrieMap


class RateCache[F[_]: Sync]() {
  
  @volatile private var cache = TrieMap[Rate.Pair, Rate]()
  private val logger = LoggerFactory.getLogger(classOf[RateCache[F]])

  def get(pair: Rate.Pair): F[Option[Rate]] = {
    Sync[F].delay {
      cache.get(pair)
    }
  }

  def put(rate: Rate): F[Unit] = {
    Sync[F].delay {
      cache.put(rate.pair, rate)
      ()
    }
  }

  def clear(): F[Unit] = Sync[F].delay {
    cache.clear()
  }
  
  def getAllCachedPairs: F[List[Rate.Pair]] = {
    Sync[F].delay(cache.keys.toList)
  }
  
  // Atomic cache replacement - prepare new cache and replace pointer
  def replaceCache(rates: List[Rate]): F[Unit] = {
    for {
      _ <- Sync[F].delay {
        // Prepare new cache with fresh data
        val newCache = TrieMap[Rate.Pair, Rate]()
        rates.foreach { rate =>
          newCache.put(rate.pair, rate)
          ()
        }
        
        // Atomic replacement - just change the pointer
        cache = newCache
      }
      _ <- Sync[F].delay(logger.info(s"Cache atomically replaced with ${rates.length} fresh rates"))
    } yield ()
  }
}