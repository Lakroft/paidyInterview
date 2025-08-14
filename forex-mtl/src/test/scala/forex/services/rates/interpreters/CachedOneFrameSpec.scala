package forex.services.rates.interpreters

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.CacheConfig
import forex.domain.{Currency, Rate}
import forex.helpers.{MockAlgebra, TestData}
import forex.services.rates.RateCache
import forex.services.rates.errors.Error.OneFrameLookupFailed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class CachedOneFrameSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "CachedOneFrame" should "return cached rate when available" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR)
    cache.put(rate).unsafeRunSync()
    
    val result = service.get(rate.pair).unsafeRunSync()
    
    result shouldBe Right(rate)
    mockClient.callCount shouldBe 0
    mockClient.batchCallCount shouldBe 0
  }
  
  it should "make batch request when expired tracked pairs exist" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(100.millis))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair1 = Rate.Pair(Currency.USD, Currency.EUR)
    val pair2 = Rate.Pair(Currency.JPY, Currency.USD)
    val rate1 = TestData.createTestRate(pair1.from, pair1.to)
    val rate2 = TestData.createTestRate(pair2.from, pair2.to)
    
    // Track pairs by requesting them
    cache.get(pair1).unsafeRunSync()
    cache.get(pair2).unsafeRunSync()
    
    // Cache rates
    cache.put(rate1).unsafeRunSync()
    cache.put(rate2).unsafeRunSync()
    
    // Wait for expiration
    Thread.sleep(150)
    
    // Setup mock expectation
    mockClient.expectBatchCall(List(pair1, pair2))
    
    val result = service.get(pair1).unsafeRunSync()
    
    result.isRight shouldBe true
    mockClient.verifyBatchCalled()
    mockClient.callCount shouldBe 0 // Should not make single calls
  }
  
  it should "make single request when no expired tracked pairs exist" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    val result = service.get(pair).unsafeRunSync()
    
    result.isRight shouldBe true
    mockClient.callCount shouldBe 1
    mockClient.batchCallCount shouldBe 0
    mockClient.calledPairs should contain(pair)
  }
  
  it should "cache rates from batch response" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(100.millis))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair1 = Rate.Pair(Currency.USD, Currency.EUR)
    val pair2 = Rate.Pair(Currency.JPY, Currency.USD)
    
    // Track pairs
    cache.get(pair1).unsafeRunSync()
    cache.get(pair2).unsafeRunSync()
    
    // Expire them (by putting expired rates)
    cache.put(TestData.createExpiredRate(pair1.from, pair1.to)).unsafeRunSync()
    cache.put(TestData.createExpiredRate(pair2.from, pair2.to)).unsafeRunSync()
    Thread.sleep(10) // Small delay to ensure expiration
    
    val result = service.get(pair1).unsafeRunSync()
    
    result.isRight shouldBe true
    
    // Both rates should now be cached from the batch response
    cache.get(pair1).unsafeRunSync().isDefined shouldBe true
    cache.get(pair2).unsafeRunSync().isDefined shouldBe true
  }
  
  it should "handle batch API failures gracefully" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(100.millis))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Track and expire pair
    cache.get(pair).unsafeRunSync()
    cache.put(TestData.createExpiredRate(pair.from, pair.to)).unsafeRunSync()
    Thread.sleep(10)
    
    mockClient.setBatchShouldFail(true)
    
    val result = service.get(pair).unsafeRunSync()
    
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe a[OneFrameLookupFailed]
  }
  
  it should "handle single API failures gracefully" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    mockClient.setShouldFail(true)
    
    val result = service.get(pair).unsafeRunSync()
    
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe a[OneFrameLookupFailed]
  }
  
  it should "return error when requested pair not found in batch response" in {
    val mockClient = new MockAlgebra[IO] {
      override def getBatch(pairs: List[Rate.Pair])(implicit F: cats.Applicative[IO]) = {
        // Return empty list instead of expected rates
        IO.pure(Right(List.empty[Rate]))
      }
    }
    val cache = new RateCache[IO](CacheConfig(100.millis))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Track and expire pair
    cache.get(pair).unsafeRunSync()
    cache.put(TestData.createExpiredRate(pair.from, pair.to)).unsafeRunSync()
    Thread.sleep(10)
    
    val result = service.get(pair).unsafeRunSync()
    
    result.isLeft shouldBe true
    result.left.getOrElse(fail()) shouldBe OneFrameLookupFailed("Pair not found in response")
  }
  
  it should "cache single API response" in {
    val mockClient = new MockAlgebra[IO]
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val service = new CachedOneFrame[IO](mockClient, cache)
    
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    val result = service.get(pair).unsafeRunSync()
    
    result.isRight shouldBe true
    
    // Rate should now be cached
    val cachedResult = service.get(pair).unsafeRunSync()
    cachedResult.isRight shouldBe true
    
    // Should have made only one API call
    mockClient.callCount shouldBe 1
  }
}