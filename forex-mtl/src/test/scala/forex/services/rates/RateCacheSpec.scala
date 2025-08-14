package forex.services.rates

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.CacheConfig
import forex.domain.{Currency, Rate}
import forex.helpers.{TestClock, TestData}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class RateCacheSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "RateCache" should "return None for non-existent pairs" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    cache.get(pair).unsafeRunSync() shouldBe None
  }
  
  it should "return cached rate when available and not expired" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR, 1.23)
    
    cache.put(rate).unsafeRunSync()
    
    val result = cache.get(rate.pair).unsafeRunSync()
    result shouldBe Some(rate)
  }
  
  it should "track requested pairs" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val pair1 = Rate.Pair(Currency.USD, Currency.EUR)
    val pair2 = Rate.Pair(Currency.JPY, Currency.USD)
    
    cache.get(pair1).unsafeRunSync()
    cache.get(pair2).unsafeRunSync()
    
    val trackedPairs = cache.getTrackedPairs.unsafeRunSync()
    trackedPairs should contain(pair1)
    trackedPairs should contain(pair2)
  }
  
  it should "expire rates after TTL" in {
    val testClock = TestClock[IO]
    val cache = new RateCache[IO](CacheConfig(2.seconds))(implicitly, testClock)
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR)
    
    cache.put(rate).unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
    
    testClock.advance(3.seconds)
    cache.get(rate.pair).unsafeRunSync() shouldBe None
  }
  
  it should "identify expired tracked pairs" in {
    val testClock = TestClock[IO]
    val cache = new RateCache[IO](CacheConfig(2.seconds))(implicitly, testClock)
    val pair1 = Rate.Pair(Currency.USD, Currency.EUR)
    val pair2 = Rate.Pair(Currency.JPY, Currency.USD)
    val rate1 = TestData.createTestRate(pair1.from, pair1.to)
    val rate2 = TestData.createTestRate(pair2.from, pair2.to)
    
    // Track pairs
    cache.get(pair1).unsafeRunSync()
    cache.get(pair2).unsafeRunSync()
    
    // Cache rates
    cache.put(rate1).unsafeRunSync()
    cache.put(rate2).unsafeRunSync()
    
    // Advance time to expire rates
    testClock.advance(3.seconds)
    
    val expiredPairs = cache.getExpiredTrackedPairs.unsafeRunSync()
    expiredPairs should contain(pair1)
    expiredPairs should contain(pair2)
  }
  
  it should "include never-cached tracked pairs in expired pairs" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // Track but don't cache
    cache.get(pair).unsafeRunSync()
    
    val expiredPairs = cache.getExpiredTrackedPairs.unsafeRunSync()
    expiredPairs should contain(pair)
  }
  
  it should "cache multiple rates in batch" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val rates = List(
      TestData.createTestRate(Currency.USD, Currency.EUR, 1.1),
      TestData.createTestRate(Currency.JPY, Currency.USD, 0.007),
      TestData.createTestRate(Currency.GBP, Currency.EUR, 1.15)
    )
    
    cache.putBatch(rates).unsafeRunSync()
    
    rates.foreach { rate =>
      cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
    }
  }
  
  it should "clear all cached data" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR)
    
    cache.put(rate).unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
    
    cache.clear().unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe None
    
    // But tracked pairs should remain
    cache.getTrackedPairs.unsafeRunSync() should contain(rate.pair)
  }
  
  it should "use putBatch for single put operation" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR)
    
    cache.put(rate).unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
  }
}