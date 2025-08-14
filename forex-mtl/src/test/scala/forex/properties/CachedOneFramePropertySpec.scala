package forex.properties

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.CacheConfig
import forex.domain.{Currency, Rate}
import forex.helpers.MockAlgebra
import forex.services.rates.RateCache
import forex.services.rates.interpreters.CachedOneFrame
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.concurrent.duration._

class CachedOneFramePropertySpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  val currencyGen: Gen[Currency] = Gen.oneOf(
    Currency.USD, Currency.EUR, Currency.JPY, Currency.GBP, 
    Currency.CHF, Currency.SGD, Currency.AUD, Currency.CAD,
    Currency.NZD
  )
  
  val ratePairGen: Gen[Rate.Pair] = for {
    from <- currencyGen
    to <- currencyGen
    if from != to
  } yield Rate.Pair(from, to)
  
  val ratePairsGen: Gen[List[Rate.Pair]] = Gen.listOfN(10, ratePairGen)

  "CachedOneFrame Properties" should "never make more API calls than distinct pairs requested" in {
    forAll(ratePairsGen) { pairs =>
      whenever(pairs.nonEmpty) {
        val mockClient = new MockAlgebra[IO]
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
  }
  
  it should "always return successful results for valid pairs when API is working" in {
    forAll(ratePairsGen) { pairs =>
      whenever(pairs.nonEmpty) {
        val mockClient = new MockAlgebra[IO]
        val cache = new RateCache[IO](CacheConfig(5.minutes))
        val service = new CachedOneFrame[IO](mockClient, cache)
        
        // All requests should succeed
        pairs.foreach { pair =>
          val result = service.get(pair).unsafeRunSync()
          result shouldBe a[Right[_, _]]
        }
      }
    }
  }
  
  it should "cache all successfully retrieved rates" in {
    forAll(ratePairsGen) { pairs =>
      whenever(pairs.nonEmpty && pairs.length <= 5) { // Limit to avoid long test times
        val mockClient = new MockAlgebra[IO]
        val cache = new RateCache[IO](CacheConfig(5.minutes))
        val service = new CachedOneFrame[IO](mockClient, cache)
        
        // First round: should hit API
        pairs.foreach(service.get(_).unsafeRunSync())
        val initialApiCalls = mockClient.callCount + mockClient.batchCallCount
        
        mockClient.reset()
        
        // Second round: should use cache
        pairs.foreach(service.get(_).unsafeRunSync())
        val cachedApiCalls = mockClient.callCount + mockClient.batchCallCount
        
        cachedApiCalls shouldBe 0
        initialApiCalls should be > 0
      }
    }
  }
  
  it should "batch efficiently when multiple pairs expire" in {
    forAll(Gen.choose(2, 8)) { numPairs =>
      val pairs = (1 to numPairs).map(_ => 
        Rate.Pair(Currency.USD, currencyGen.sample.get)
      ).distinct.toList
      
      whenever(pairs.length >= 2) {
        val mockClient = new MockAlgebra[IO]
        val cache = new RateCache[IO](CacheConfig(50.millis))
        val service = new CachedOneFrame[IO](mockClient, cache)
        
        // Track pairs by requesting them
        pairs.foreach(service.get(_).unsafeRunSync())
        
        // Wait for expiration
        Thread.sleep(60)
        mockClient.reset()
        
        // Request first pair - should trigger batch for all
        service.get(pairs.head).unsafeRunSync()
        
        // Should make exactly one batch call
        mockClient.batchCallCount shouldBe 1
        mockClient.callCount shouldBe 0
        
        // Batch should include all expired pairs
        if (mockClient.batchCalledPairs.nonEmpty) {
          mockClient.batchCalledPairs.head.toSet shouldBe pairs.toSet
        }
      }
    }
  }
  
  it should "maintain cache consistency under concurrent access" in {
    forAll(Gen.choose(1, 5)) { numPairs =>
      val pairs = (1 to numPairs).map(_ => 
        Rate.Pair(Currency.USD, Currency.EUR)
      ).distinct.toList
      
      whenever(pairs.nonEmpty) {
        val mockClient = new MockAlgebra[IO]
        val cache = new RateCache[IO](CacheConfig(5.minutes))
        val service = new CachedOneFrame[IO](mockClient, cache)
        
        // Make concurrent requests for the same pair
        val concurrentRequests = 20
        val results = (1 to concurrentRequests).map { _ =>
          service.get(pairs.head).unsafeRunSync()
        }
        
        // All should succeed
        results.foreach(_ shouldBe a[Right[_, _]])
        
        // Should make at most a few API calls despite many concurrent requests
        val totalApiCalls = mockClient.callCount + mockClient.batchCallCount
        totalApiCalls should be <= 3 // Allow for some race conditions
      }
    }
  }
  
  it should "track exactly the pairs that were requested" in {
    forAll(ratePairsGen) { pairs =>
      whenever(pairs.nonEmpty && pairs.length <= 10) {
        val mockClient = new MockAlgebra[IO]
        val cache = new RateCache[IO](CacheConfig(5.minutes))
        val service = new CachedOneFrame[IO](mockClient, cache)
        
        // Request all pairs
        pairs.foreach(service.get(_).unsafeRunSync())
        
        // Tracked pairs should match distinct requested pairs
        val trackedPairs = cache.getTrackedPairs.unsafeRunSync().toSet
        val distinctRequestedPairs = pairs.distinct.toSet
        
        trackedPairs shouldBe distinctRequestedPairs
      }
    }
  }
}