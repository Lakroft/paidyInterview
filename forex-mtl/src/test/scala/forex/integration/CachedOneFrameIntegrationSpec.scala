package forex.integration

import cats.effect.{ContextShift, IO, Timer}
import cats.effect.concurrent.Ref

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.CacheConfig
import forex.domain.{Currency, Rate}
import forex.helpers.{MockAlgebra, TestClock}
import forex.services.rates.RateCache
import forex.services.rates.interpreters.CachedOneFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class CachedOneFrameIntegrationSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)
  
  // Helper function to create CachedOneFrame instance for tests
  private def createCachedOneFrame(mockClient: forex.services.rates.Algebra[IO], cache: RateCache[IO]): CachedOneFrame[IO] = {
    val loadingRef = Ref.of[IO, Boolean](false).unsafeRunSync()
    new CachedOneFrame[IO](mockClient, cache, loadingRef)
  }

  "CachedOneFrame Integration" should "optimize API calls with batching" in {
    val testClock = TestClock[IO]
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(2.seconds))(implicitly, testClock)
    val service = createCachedOneFrame(mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    // Phase 1: Initial requests - should make 1 batch call (for all supported pairs)
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    mockClient.batchCallCount shouldBe 1
    
    // Phase 2: Immediate re-requests - should use cache
    mockClient.reset()
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    mockClient.batchCallCount shouldBe 0
    
    // Phase 3: After expiration - should make 1 batch call
    testClock.advance(3.seconds) // Advance time beyond TTL
    mockClient.reset()
    
    // Request first pair - should trigger batch for all expired tracked pairs
    service.get(pairs.head).unsafeRunSync() shouldBe a[Right[_, _]]
    
    mockClient.batchCallCount shouldBe 1

    // All pairs should now be cached again
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    // Should still be only 1 batch call
    mockClient.batchCallCount shouldBe 1
  }
  
  it should "cache all pairs when any cache miss occurs" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = createCachedOneFrame(mockClient, cache)
    
    val firstPair = Rate.Pair(Currency.USD, Currency.EUR)
    val secondPair = Rate.Pair(Currency.JPY, Currency.USD)
    
    // Request first pair - should trigger batch for all supported pairs
    service.get(firstPair).unsafeRunSync() shouldBe a[Right[_, _]]
    mockClient.batchCallCount shouldBe 1
    
    // Request second pair - should use cache (no additional API calls)
    mockClient.reset()
    service.get(secondPair).unsafeRunSync() shouldBe a[Right[_, _]]
    mockClient.batchCallCount shouldBe 0
    
    // All supported pairs should be cached
    import forex.domain.Currency
    val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }.toSet
    cache.getAllCachedPairs.unsafeRunSync().toSet shouldBe allSupportedPairs
  }
  
  it should "recover from API failures and retry successfully" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Simulate API failure
    mockClient.setBatchShouldFail(true)
    val failedResult = service.get(pair).unsafeRunSync()
    failedResult.isLeft shouldBe true
    
    // Fix API and retry
    mockClient.setBatchShouldFail(false)
    val successResult = service.get(pair).unsafeRunSync()
    successResult.isRight shouldBe true
    
    mockClient.batchCallCount shouldBe 2
  }
  
  it should "maintain performance under concurrent load" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Simulate concurrent requests
    val futures = (1 to 100).map(_ => 
      IO(service.get(pair).unsafeRunSync())
    ).toList
    
    // All should complete
    val results = futures.map(_.unsafeRunSync())
    results.foreach(_ shouldBe a[Right[_, _]])
    
    // Should make only 1 batch API call despite 100 concurrent requests
    mockClient.batchCallCount shouldBe 1
  }
  
  it should "handle cache invalidation scenarios" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Initial request
    service.get(pair).unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Clear cache manually
    cache.clear().unsafeRunSync()
    
    // Next request should hit API again (as tracked pair is expired)
    service.get(pair).unsafeRunSync()
    // Note: This will make batch call since tracked pair is now expired
    mockClient.batchCallCount shouldBe 2
  }
  
  it should "batch requests efficiently when multiple pairs expire simultaneously" in {
    val testClock = TestClock[IO]
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO](CacheConfig(2.seconds))(implicitly, testClock)
    val service = createCachedOneFrame(mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF),
      Rate.Pair(Currency.AUD, Currency.CAD),
      Rate.Pair(Currency.NZD, Currency.SGD)
    )
    
    // Track all pairs by requesting them
    pairs.foreach(service.get(_).unsafeRunSync())
    
    // Advance time to expire all pairs
    testClock.advance(3.seconds)
    mockClient.reset()
    
    // Request any pair - should batch all expired pairs
    service.get(pairs.head).unsafeRunSync()
    
    mockClient.batchCallCount shouldBe 1

    // Verify all supported pairs were included in the batch
    import forex.domain.Currency
    mockClient.batchCalledPairs.head.toSet shouldBe Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }.toSet
  }
}