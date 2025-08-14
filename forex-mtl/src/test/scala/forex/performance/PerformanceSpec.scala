package forex.performance

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.CacheConfig
import forex.domain.{Currency, Rate}
import forex.helpers.MockAlgebra
import forex.services.rates.RateCache
import forex.services.rates.interpreters.CachedOneFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class PerformanceSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "CachedOneFrame Performance" should "handle 1000 requests efficiently with caching" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    val requestCount = 1000
    
    val startTime = System.currentTimeMillis()
    
    // Make 1000 requests
    (1 to requestCount).foreach { _ =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    
    val duration = System.currentTimeMillis() - startTime
    
    // Should complete quickly (under 1 second for 1000 cached requests)
    duration should be < 1000L
    
    // Should make only 1 API call despite 1000 requests
    mockClient.batchCallCount shouldBe 1
  }
  
  it should "efficiently batch requests for multiple pairs" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(100.millis))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.EUR, Currency.JPY),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.USD),
      Rate.Pair(Currency.CHF, Currency.SGD),
      Rate.Pair(Currency.AUD, Currency.CAD),
      Rate.Pair(Currency.NZD, Currency.GBP),
    )
    
    // Initial requests to track pairs
    pairs.foreach(service.get(_).unsafeRunSync())
    
    // Wait for expiration
    Thread.sleep(150)
    mockClient.reset()
    
    val startTime = System.currentTimeMillis()
    
    // Request all pairs - should trigger one batch call
    pairs.foreach(service.get(_).unsafeRunSync())
    
    val duration = System.currentTimeMillis() - startTime
    
    // Should complete quickly
    duration should be < 500L
    
    // Should make exactly 1 batch call for all pairs
    mockClient.batchCallCount shouldBe 1
    mockClient.callCount shouldBe 0
  }
  
  it should "maintain performance under memory pressure" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    // Create many different pairs to test memory usage
    val currencies = List(Currency.USD, Currency.EUR, Currency.JPY, Currency.GBP, Currency.CHF, Currency.SGD, Currency.AUD, Currency.CAD)
    val pairs = for {
      from <- currencies
      to <- currencies
      if from != to
    } yield Rate.Pair(from, to)
    
    val startTime = System.currentTimeMillis()
    
    // Request all pairs twice
    pairs.foreach(service.get(_).unsafeRunSync())
    pairs.foreach(service.get(_).unsafeRunSync())
    
    val duration = System.currentTimeMillis() - startTime
    
    // Should complete reasonably quickly
    duration should be < 2000L
    
    // First round should make API calls, second round should be cached
    mockClient.batchCallCount shouldBe pairs.length
    
    // Verify tracked pairs are managed efficiently
    cache.getTrackedPairs.unsafeRunSync().length shouldBe pairs.length
  }
  
  it should "handle rapid cache expiration cycles efficiently" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(50.millis))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    val cycles = 5
    var totalApiCalls = 0
    
    val startTime = System.currentTimeMillis()
    
    (1 to cycles).foreach { cycle =>
      mockClient.reset()
      
      // Request all pairs
      pairs.foreach(service.get(_).unsafeRunSync())
      
      if (cycle == 1) {
        // First cycle: individual calls
        totalApiCalls += mockClient.callCount
      } else {
        // Subsequent cycles: should batch
        totalApiCalls += mockClient.batchCallCount
      }
      
      // Wait for expiration
      Thread.sleep(60)
    }
    
    val duration = System.currentTimeMillis() - startTime
    
    // Should complete in reasonable time
    duration should be < (cycles * 200L)
    
    // Should use batching efficiently after first cycle
    totalApiCalls should be <= (pairs.length + cycles - 1)
  }
}