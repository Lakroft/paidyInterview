package forex.services.rates.interpreters

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.domain.{Currency, Rate}
import forex.helpers.{MockAlgebra, TestClock, TestData}
import forex.services.rates.RateCache
import forex.services.rates.errors.Error.{InvalidCurrencyPair, OneFrameLookupFailed, RateNotFound}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CachedOneFrameSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)
  
  // Helper function to create CachedOneFrame instance for tests
  private def createCachedOneFrame(mockClient: forex.services.rates.Algebra[IO], cache: RateCache[IO]): CachedOneFrame[IO] = {
    new CachedOneFrame[IO](mockClient, cache)
  }

  "CachedOneFrame" should "return cached rate when available" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val rate = TestData.createTestRateWithClock(Currency.USD, Currency.EUR, testClock)
    cache.put(rate).unsafeRunSync()
    
    val result = service.get(rate.pair).unsafeRunSync()
    
    result shouldBe Right(rate)
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "return RateNotFound when cache is empty" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Request from empty cache should return RateNotFound
    val result = service.get(pair).unsafeRunSync()
    
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe RateNotFound("USDEUR")
    
    // Should not make any API calls for user requests
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "serve from cache when refreshCache has populated it" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Initially should return RateNotFound
    service.get(pair).unsafeRunSync().isLeft shouldBe true
    
    // Populate cache using refreshCache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Reset mock to track subsequent calls
    mockClient.reset()
    
    // Now user request should succeed from cache
    val result = service.get(pair).unsafeRunSync()
    result.isRight shouldBe true
    
    // Should not make additional API calls
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "cache all supported pairs from refreshCache" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair1 = Rate.Pair(Currency.USD, Currency.EUR)
    val pair2 = Rate.Pair(Currency.JPY, Currency.USD)
    
    // Initially both pairs should return RateNotFound
    service.get(pair1).unsafeRunSync().isLeft shouldBe true
    service.get(pair2).unsafeRunSync().isLeft shouldBe true
    
    // Use refreshCache to populate all supported pairs
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Now both rates should be available from cache
    val result1 = service.get(pair1).unsafeRunSync()
    val result2 = service.get(pair2).unsafeRunSync()
    
    result1.isRight shouldBe true
    result2.isRight shouldBe true
    
    // All supported pairs should be cached
    import forex.domain.Currency
    val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }.toSet
    cache.getAllCachedPairs.unsafeRunSync().toSet shouldBe allSupportedPairs
  }
  
  it should "handle refreshCache API failures gracefully" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Simulate API failure during refreshCache
    mockClient.setBatchShouldFail(true)
    
    val refreshResult = service.refreshCache().unsafeRunSync()
    refreshResult.isLeft shouldBe true
    refreshResult.left.getOrElse(fail()) shouldBe a[OneFrameLookupFailed]
    
    // User request should return RateNotFound since cache is still empty
    val result = service.get(pair).unsafeRunSync()
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe RateNotFound("USDEUR")
  }
  
  it should "demonstrate timer-based refresh pattern" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Initially should return RateNotFound
    service.get(pair).unsafeRunSync().isLeft shouldBe true
    mockClient.batchCallCount shouldBe 0
    
    // Simulate timer-based refresh (this would normally be called by Main.scala timer)
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Now user request should succeed from cache
    val result = service.get(pair).unsafeRunSync()
    result.isRight shouldBe true
    
    // Additional user requests should continue to work from cache
    service.get(pair).unsafeRunSync().isRight shouldBe true
    
    // Should not make additional API calls for user requests
    mockClient.batchCallCount shouldBe 1
  }
  
  it should "handle empty batch response in refreshCache" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock)) {
      override def getBatch(pairs: List[Rate.Pair]) = {
        // Return empty list instead of expected rates
        IO.pure(Right(List.empty[Rate]))
      }
    }
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // refreshCache with empty response should succeed but not populate cache
    val refreshResult = service.refreshCache().unsafeRunSync()
    refreshResult.isRight shouldBe true
    
    // User request should still return RateNotFound since cache is empty
    val result = service.get(pair).unsafeRunSync()
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe RateNotFound("USDEUR")
  }
  
  it should "maintain cache consistency across multiple refreshes" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // First refresh
    service.refreshCache().unsafeRunSync()
    val result1 = service.get(pair).unsafeRunSync()
    result1.isRight shouldBe true
    
    // Second refresh - should replace cache atomically
    service.refreshCache().unsafeRunSync()
    val result2 = service.get(pair).unsafeRunSync()
    result2.isRight shouldBe true
    
    // Should have made two refresh API calls
    mockClient.batchCallCount shouldBe 2
  }
  
  it should "reject same currency pairs early" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val samePair = Rate.Pair(Currency.USD, Currency.USD)
    
    val result = service.get(samePair).unsafeRunSync()
    
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe InvalidCurrencyPair("USDUSD", "same currency conversion not supported")
    
    // Should not call API or access cache for invalid pairs
    mockClient.batchCallCount shouldBe 0
    mockClient.callCount shouldBe 0
  }
  
  it should "not process invalid pairs and not make API calls" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    // Try to get invalid pair - should return InvalidCurrencyPair error
    val result = service.get(Rate.Pair(Currency.EUR, Currency.EUR)).unsafeRunSync()
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe InvalidCurrencyPair("EUREUR", "same currency conversion not supported")
    
    // Should not be cached
    val cachedPairs = cache.getAllCachedPairs.unsafeRunSync()
    cachedPairs should not contain Rate.Pair(Currency.EUR, Currency.EUR)
    
    // Should not make API calls
    mockClient.batchCallCount shouldBe 0
  }
}