package forex.integration

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
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
    new CachedOneFrame[IO](mockClient, cache)
  }

  "CachedOneFrame Integration" should "return RateNotFound when cache is empty" in {
    val testClock = TestClock[IO]
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    // Phase 1: Requests with empty cache - should return RateNotFound
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Left[_, _]]
    }
    // No API calls should be made from user requests
    mockClient.batchCallCount shouldBe 0
    
    // Phase 2: Pre-populate cache via refreshCache method and then test reads
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Now requests should hit cache
    mockClient.reset()
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    mockClient.batchCallCount shouldBe 0
    
    // Phase 3: Cache persists (no automatic expiration)
    testClock.advance(3.seconds) // Advance time - cache should still be valid
    
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    // No additional API calls should be made
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "use refreshCache to populate cache with all supported pairs" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val firstPair = Rate.Pair(Currency.USD, Currency.EUR)
    val secondPair = Rate.Pair(Currency.JPY, Currency.USD)
    
    // Initially, requests should return RateNotFound
    service.get(firstPair).unsafeRunSync() shouldBe a[Left[_, _]]
    service.get(secondPair).unsafeRunSync() shouldBe a[Left[_, _]]
    mockClient.batchCallCount shouldBe 0
    
    // Use refreshCache to populate cache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Now both requests should succeed from cache
    mockClient.reset()
    service.get(firstPair).unsafeRunSync() shouldBe a[Right[_, _]]
    service.get(secondPair).unsafeRunSync() shouldBe a[Right[_, _]]
    mockClient.batchCallCount shouldBe 0
    
    // All supported pairs should be cached
    import forex.domain.Currency
    val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }.toSet
    cache.getAllCachedPairs.unsafeRunSync().toSet shouldBe allSupportedPairs
  }
  
  it should "handle API failures in refreshCache gracefully" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Simulate API failure during cache refresh
    mockClient.setBatchShouldFail(true)
    val failedRefreshResult = service.refreshCache().unsafeRunSync()
    failedRefreshResult.isLeft shouldBe true
    
    // User requests should still return RateNotFound (cache remains empty)
    val userRequestResult = service.get(pair).unsafeRunSync()
    userRequestResult.isLeft shouldBe true
    
    // Fix API and retry refresh
    mockClient.setBatchShouldFail(false)
    val successRefreshResult = service.refreshCache().unsafeRunSync()
    successRefreshResult.isRight shouldBe true
    
    // Now user requests should succeed
    val successUserResult = service.get(pair).unsafeRunSync()
    successUserResult.isRight shouldBe true
    
    mockClient.batchCallCount shouldBe 2
  }
  
  it should "maintain performance under concurrent load with pre-populated cache" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Pre-populate cache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    mockClient.reset()
    
    // Simulate concurrent requests
    val futures = (1 to 100).map(_ => 
      IO(service.get(pair).unsafeRunSync())
    ).toList
    
    // All should complete successfully from cache
    val results = futures.map(_.unsafeRunSync())
    results.foreach(_ shouldBe a[Right[_, _]])
    
    // Should make 0 additional API calls since everything comes from cache
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "handle cache replacement scenarios" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Populate cache first
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Verify cache works
    service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    
    // Clear cache manually 
    cache.clear().unsafeRunSync()
    
    // Next request should return RateNotFound (cache is empty)
    service.get(pair).unsafeRunSync() shouldBe a[Left[_, _]]
    
    // No additional API calls from user requests
    mockClient.batchCallCount shouldBe 1
  }
  
  it should "demonstrate timer-based cache refresh pattern" in {
    val testClock = TestClock[IO]
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF),
      Rate.Pair(Currency.AUD, Currency.CAD),
      Rate.Pair(Currency.NZD, Currency.SGD)
    )
    
    // Initially all pairs return RateNotFound
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Left[_, _]]
    }
    mockClient.batchCallCount shouldBe 0
    
    // Simulate timer-based refresh
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Now all pairs should be available from cache
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    // No additional API calls
    mockClient.batchCallCount shouldBe 1
  }

}