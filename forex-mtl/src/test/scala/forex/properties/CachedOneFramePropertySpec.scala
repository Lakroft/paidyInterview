package forex.properties

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.domain.{Currency, Rate}
import forex.helpers.{MockAlgebra, TestClock}
import forex.services.rates.RateCache
import forex.services.rates.interpreters.CachedOneFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CachedOneFramePropertySpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)
  
  // Helper function to create CachedOneFrame instance for tests
  private def createCachedOneFrame(mockClient: forex.services.rates.Algebra[IO], cache: RateCache[IO]): CachedOneFrame[IO] = {
    new CachedOneFrame[IO](mockClient, cache)
  }

  "CachedOneFrame Properties" should "return RateNotFound for user requests when cache is empty" in {
    val testCases = List(
      List(Rate.Pair(Currency.USD, Currency.EUR)),
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.JPY, Currency.USD)),
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.JPY, Currency.USD), Rate.Pair(Currency.GBP, Currency.CHF)),
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.USD, Currency.EUR)) // duplicate
    )
    
    testCases.foreach { pairs =>
      val testClock = new TestClock[IO]

      val mockClient = new MockAlgebra[IO](Some(testClock))
      val cache = new RateCache[IO]()
      val service = createCachedOneFrame(mockClient, cache)
      
      // Request all pairs from empty cache should return RateNotFound
      pairs.foreach { pair =>
        val result = service.get(pair).unsafeRunSync()
        result.isLeft shouldBe true
      }
      
      // Should not make any API calls for user requests
      val totalApiCalls = mockClient.callCount + mockClient.batchCallCount
      totalApiCalls shouldBe 0
    }
  }
  
  it should "return successful results for valid pairs when cache is populated" in {
    val testPairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF),
      Rate.Pair(Currency.AUD, Currency.CAD)
    )
    
    val testClock = new TestClock[IO]

    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    // Pre-populate cache using refreshCache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Reset mock to track subsequent calls
    mockClient.reset()
    
    // All requests should now succeed from cache
    testPairs.foreach { pair =>
      val result = service.get(pair).unsafeRunSync()
      result shouldBe a[Right[_, _]]
    }
    
    // Should not make additional API calls
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "serve all rates from cache after refreshCache" in {
    val testPairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    val testClock = new TestClock[IO]

    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    // Initially should return RateNotFound
    testPairs.foreach { pair =>
      service.get(pair).unsafeRunSync().isLeft shouldBe true
    }
    
    // Use refreshCache to populate cache
    service.refreshCache().unsafeRunSync()
    val refreshApiCalls = mockClient.batchCallCount
    refreshApiCalls shouldBe 1
    
    mockClient.reset()
    
    // Now should serve from cache
    testPairs.foreach { pair =>
      service.get(pair).unsafeRunSync().isRight shouldBe true
    }
    val cachedApiCalls = mockClient.callCount + mockClient.batchCallCount
    
    cachedApiCalls shouldBe 0
  }
  
  it should "handle multiple refresh cycles efficiently" in {
    // Use deterministic pairs instead of random generation
    val testPairs = List(
      List(Rate.Pair(Currency.USD, Currency.EUR), Rate.Pair(Currency.USD, Currency.JPY)),
      List(Rate.Pair(Currency.USD, Currency.GBP), Rate.Pair(Currency.USD, Currency.CHF), Rate.Pair(Currency.USD, Currency.SGD)),
      List(Rate.Pair(Currency.EUR, Currency.JPY), Rate.Pair(Currency.EUR, Currency.GBP), Rate.Pair(Currency.EUR, Currency.CHF), Rate.Pair(Currency.EUR, Currency.AUD))
    )
    
    testPairs.foreach { validPairs =>
      val testClock = new TestClock[IO]

      val mockClient = new MockAlgebra[IO](Some(testClock))
      val cache = new RateCache[IO]()
      val service = createCachedOneFrame(mockClient, cache)
      
      // Initially user requests should return RateNotFound
      validPairs.foreach { pair =>
        service.get(pair).unsafeRunSync().isLeft shouldBe true
      }
      mockClient.batchCallCount shouldBe 0
      
      // Simulate timer-based refresh
      service.refreshCache().unsafeRunSync()
      mockClient.batchCallCount shouldBe 1
      
      // Batch should include all supported pairs
      if (mockClient.batchCalledPairs.nonEmpty) {
        import forex.domain.Currency
        val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }
        mockClient.batchCalledPairs.head.toSet shouldBe allSupportedPairs.toSet
      }
      
      // Reset and test another refresh cycle
      mockClient.reset()
      service.refreshCache().unsafeRunSync()
      mockClient.batchCallCount shouldBe 1
    }
  }
  
  it should "maintain cache consistency under concurrent access with pre-populated cache" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Pre-populate cache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Reset mock to track user requests
    mockClient.reset()
    
    // Make concurrent requests for the same pair
    val concurrentRequests = 20
    val results = (1 to concurrentRequests).map { _ =>
      service.get(pair).unsafeRunSync()
    }
    
    // All should succeed from cache
    results.foreach(_ shouldBe a[Right[_, _]])
    
    // Should make no API calls for user requests
    val totalApiCalls = mockClient.callCount + mockClient.batchCallCount
    totalApiCalls shouldBe 0
  }
  
  it should "cache all supported pairs after refreshCache regardless of user requests" in {
    val testPairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF),
      Rate.Pair(Currency.USD, Currency.EUR) // duplicate
    )
    
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    // Initially, user requests should return RateNotFound
    testPairs.foreach { pair =>
      service.get(pair).unsafeRunSync().isLeft shouldBe true
    }
    
    // Cache should be empty
    cache.getAllCachedPairs.unsafeRunSync().toSet shouldBe Set.empty
    
    // Use refreshCache to populate cache
    service.refreshCache().unsafeRunSync()
    
    // Cached pairs should match all supported pairs (since refreshCache loads everything)
    val cachedPairs = cache.getAllCachedPairs.unsafeRunSync().toSet
    import forex.domain.Currency
    val allSupportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }.toSet
    
    cachedPairs shouldBe allSupportedPairs
  }
}