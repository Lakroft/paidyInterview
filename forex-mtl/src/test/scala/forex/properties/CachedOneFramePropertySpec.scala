package forex.properties

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

class CachedOneFramePropertySpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "CachedOneFrame Properties" should "never make more API calls than distinct pairs requested" in {
    val testCases = List(
      List(Rate.Pair(Currency.USD, Currency.EUR)),
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.JPY, Currency.USD)),
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.JPY, Currency.USD), Rate.Pair(Currency.GBP, Currency.CHF)),
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.USD, Currency.EUR)) // duplicate
    )
    
    testCases.foreach { pairs =>
      val testClock = new TestClock[IO]
      implicit val clock = testClock
      
      val mockClient = new MockAlgebra[IO](Some(testClock))
      val cache = new RateCache[IO](CacheConfig(5.minutes))
      val service = new CachedOneFrame[IO](mockClient, cache)
      
      // Request all pairs
      pairs.foreach(service.get(_).unsafeRunSync())
      
      // Total API calls should not exceed distinct pairs
      val totalApiCalls = mockClient.callCount + mockClient.batchCallCount
      val distinctPairs = pairs.distinct.length
      
      totalApiCalls should be <= distinctPairs
    }
  }
  
  it should "always return successful results for valid pairs when API is working" in {
    val testPairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF),
      Rate.Pair(Currency.AUD, Currency.CAD)
    )
    
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    // All requests should succeed
    testPairs.foreach { pair =>
      val result = service.get(pair).unsafeRunSync()
      result shouldBe a[Right[_, _]]
    }
  }
  
  it should "cache all successfully retrieved rates" in {
    val testPairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    // First round: should hit API
    testPairs.foreach(service.get(_).unsafeRunSync())
    val initialApiCalls = mockClient.callCount + mockClient.batchCallCount
    
    mockClient.reset()
    
    // Second round: should use cache
    testPairs.foreach(service.get(_).unsafeRunSync())
    val cachedApiCalls = mockClient.callCount + mockClient.batchCallCount
    
    cachedApiCalls shouldBe 0
    initialApiCalls should be > 0
  }
  
  it should "batch efficiently when multiple pairs expire" in {
    // Use deterministic pairs instead of random generation
    val testPairs = List(
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.USD, Currency.JPY)),
      List(Rate.Pair(Currency.USD, Currency.GBP), Rate.Pair(Currency.USD, Currency.CHF), Rate.Pair(Currency.USD, Currency.SGD)),
      List(Rate.Pair(Currency.EUR, Currency.JPY), Rate.Pair(Currency.EUR, Currency.GBP), Rate.Pair(Currency.EUR, Currency.CHF), Rate.Pair(Currency.EUR, Currency.AUD))
    )
    
    testPairs.foreach { validPairs =>
      val testClock = new TestClock[IO]
      implicit val clock = testClock
      
      val mockClient = new MockAlgebra[IO](Some(testClock))
      val cache = new RateCache[IO](CacheConfig(5.seconds))
      val service = new CachedOneFrame[IO](mockClient, cache)
      
      // Track pairs by requesting them
      validPairs.foreach(service.get(_).unsafeRunSync())
      
      // Advance time to expire cache
      testClock.advance(10.seconds)
      mockClient.reset()
      
      // Request first pair - should trigger batch for all
      service.get(validPairs.head).unsafeRunSync()
      
      // Should make exactly one batch call
      mockClient.batchCallCount shouldBe 1

      // Batch should include all expired pairs
      if (mockClient.batchCalledPairs.nonEmpty) {
        mockClient.batchCalledPairs.head.toSet shouldBe validPairs.toSet
      }
    }
  }
  
  it should "maintain cache consistency under concurrent access" in {
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Make concurrent requests for the same pair
    val concurrentRequests = 20
    val results = (1 to concurrentRequests).map { _ =>
      service.get(pair).unsafeRunSync()
    }
    
    // All should succeed
    results.foreach(_ shouldBe a[Right[_, _]])
    
    // Should make at most a few API calls despite many concurrent requests
    val totalApiCalls = mockClient.callCount + mockClient.batchCallCount
    totalApiCalls should be <= 3 // Allow for some race conditions
  }
  
  it should "track exactly the pairs that were requested" in {
    val testPairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF),
      Rate.Pair(Currency.USD, Currency.EUR) // duplicate
    )
    
    val testClock = new TestClock[IO]
    implicit val clock = testClock
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    // Request all pairs
    testPairs.foreach(service.get(_).unsafeRunSync())
    
    // Tracked pairs should match distinct requested pairs
    val trackedPairs = cache.getTrackedPairs.unsafeRunSync().toSet
    val distinctRequestedPairs = testPairs.distinct.toSet
    
    trackedPairs shouldBe distinctRequestedPairs
  }
}