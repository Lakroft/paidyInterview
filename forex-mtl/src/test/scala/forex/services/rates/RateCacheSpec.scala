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
  
  it should "return all cached pairs" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val rate1 = TestData.createTestRate(Currency.USD, Currency.EUR)
    val rate2 = TestData.createTestRate(Currency.JPY, Currency.USD)
    
    cache.put(rate1).unsafeRunSync()
    cache.put(rate2).unsafeRunSync()
    
    val cachedPairs = cache.getAllCachedPairs.unsafeRunSync()
    cachedPairs should contain(rate1.pair)
    cachedPairs should contain(rate2.pair)
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
  
  it should "return empty list when no pairs are cached" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    
    val cachedPairs = cache.getAllCachedPairs.unsafeRunSync()
    cachedPairs shouldBe List.empty
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
    cache.getAllCachedPairs.unsafeRunSync() shouldBe List.empty
  }
  
  it should "use putBatch for single put operation" in {
    val cache = new RateCache[IO](CacheConfig(5.minutes))
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR)
    
    cache.put(rate).unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
  }
}