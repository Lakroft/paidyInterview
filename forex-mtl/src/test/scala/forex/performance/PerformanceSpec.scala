package forex.performance

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.CacheConfig
import forex.domain.{Currency, Rate}
import forex.helpers.{MockAlgebra, TestClock}
import forex.services.rates.RateCache
import forex.services.rates.interpreters.CachedOneFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class PerformanceSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "CachedOneFrame Performance" should "handle 1000 requests efficiently with caching" in {
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    val requestCount = 1000
    
    // Make 1000 requests - should be fast with caching
    (1 to requestCount).foreach { _ =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    
    // Should make only 1 API call despite 1000 requests
    mockClient.batchCallCount shouldBe 1
  }
  
  it should "efficiently batch requests for multiple pairs" in {
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.seconds))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.EUR, Currency.JPY),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.USD),
      Rate.Pair(Currency.CHF, Currency.SGD),
      Rate.Pair(Currency.AUD, Currency.CAD),
      Rate.Pair(Currency.NZD, Currency.GBP)
    )
    
    // Initial requests to track pairs
    pairs.foreach(service.get(_).unsafeRunSync())
    
    // Expire cache by advancing time
    testClock.advance(10.seconds)
    mockClient.reset()
    
    // Request all pairs - should trigger one batch call
    pairs.foreach(service.get(_).unsafeRunSync())
    
    // Should make exactly 1 batch call for all pairs
    mockClient.batchCallCount shouldBe 1
  }
  
  it should "maintain performance under memory pressure" in {
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    // Create many different pairs to test memory usage
    val currencies = List(Currency.USD, Currency.EUR, Currency.JPY, Currency.GBP, Currency.CHF, Currency.SGD, Currency.AUD, Currency.CAD, Currency.NZD)
    val pairs = for {
      from <- currencies
      to <- currencies
      if from != to
    } yield Rate.Pair(from, to)
    
    // Request all pairs twice
    pairs.foreach(service.get(_).unsafeRunSync())
    pairs.foreach(service.get(_).unsafeRunSync())
    
    // First round should make API calls, second round should be cached
    mockClient.batchCallCount shouldBe pairs.length
    
    // Verify tracked pairs are managed efficiently
    cache.getTrackedPairs.unsafeRunSync().length shouldBe pairs.length
  }
  
  it should "handle rapid cache expiration cycles efficiently" in {
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.seconds))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    val cycles = 5
    var totalApiCalls = 0
    
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
      
      // Advance time to expire cache
      testClock.advance(10.seconds)
    }
    
    // Should use batching efficiently after first cycle
    totalApiCalls should be <= (pairs.length + cycles - 1)
  }
}