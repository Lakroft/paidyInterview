package forex.performance

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.domain.{Currency, Rate}
import forex.helpers.{MockAlgebra, TestClock}
import forex.services.rates.RateCache
import forex.services.rates.interpreters.CachedOneFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class PerformanceSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)
  
  // Helper function to create CachedOneFrame instance for tests
  private def createCachedOneFrame(mockClient: forex.services.rates.Algebra[IO], cache: RateCache[IO]): CachedOneFrame[IO] = {
    new CachedOneFrame[IO](mockClient, cache)
  }

  "CachedOneFrame Performance" should "handle 1000 requests efficiently with caching" in {
    val testClock = new TestClock[IO]

    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    val requestCount = 1000
    
    // Pre-populate cache using refreshCache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Reset mock to track subsequent calls
    mockClient.reset()
    
    // Make 1000 requests - should be fast with caching
    (1 to requestCount).foreach { _ =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    
    // Should make 0 additional API calls (all from cache)
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "efficiently serve multiple pairs from cache" in {
    val testClock = new TestClock[IO]

    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.EUR, Currency.JPY),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.USD),
      Rate.Pair(Currency.CHF, Currency.SGD),
      Rate.Pair(Currency.AUD, Currency.CAD),
      Rate.Pair(Currency.NZD, Currency.GBP)
    )
    
    // Pre-populate cache using refreshCache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Reset mock to track subsequent calls
    mockClient.reset()
    
    // Request all pairs - should all come from cache
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    
    // Should make 0 additional API calls (all from cache)
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "maintain performance under memory pressure" in {
    val testClock = new TestClock[IO]
    
    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    // Create many different pairs to test memory usage
    val currencies = Currency.allCurrencies.toList
    val pairs = for {
      from <- currencies
      to <- currencies
      if from != to
    } yield Rate.Pair(from, to)
    
    // Pre-populate cache using refreshCache
    service.refreshCache().unsafeRunSync()
    mockClient.batchCallCount shouldBe 1
    
    // Reset mock to track subsequent calls
    mockClient.reset()
    
    // Request all pairs twice - should all come from cache
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    pairs.foreach { pair =>
      service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
    }
    
    // Should make 0 additional API calls (all from cache)
    mockClient.batchCallCount shouldBe 0
    
    // Verify all supported pairs are cached efficiently
    val supportedPairs = Currency.supportedPairs.map { case (from, to) => Rate.Pair(from, to) }.toSet
    cache.getAllCachedPairs.unsafeRunSync().toSet shouldBe supportedPairs
  }
  
  it should "handle rapid cache refresh cycles efficiently" in {
    val testClock = new TestClock[IO]

    val mockClient = new MockAlgebra[IO](Some(testClock))
    val cache = new RateCache[IO]()
    val service = createCachedOneFrame(mockClient, cache)
    
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    val cycles = 5
    
    (1 to cycles).foreach { _ =>
      // Refresh cache - simulates timer-based refresh
      service.refreshCache().unsafeRunSync()
      
      // Request all pairs - should all come from cache
      pairs.foreach { pair =>
        service.get(pair).unsafeRunSync() shouldBe a[Right[_, _]]
      }
    }
    
    // Should make exactly 5 batch calls (one per refresh cycle)
    mockClient.batchCallCount shouldBe cycles
    
    // Verify all pairs are still cached after multiple refresh cycles
    pairs.foreach { pair =>
      cache.get(pair).unsafeRunSync().isDefined shouldBe true
    }
  }
}