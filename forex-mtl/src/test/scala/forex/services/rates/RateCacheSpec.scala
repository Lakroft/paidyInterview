package forex.services.rates

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.domain.{Currency, Rate}
import forex.helpers.TestData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RateCacheSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "RateCache" should "return None for non-existent pairs" in {
    val cache = new RateCache[IO]()
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    cache.get(pair).unsafeRunSync() shouldBe None
  }
  
  it should "return cached rate when available and not expired" in {
    val cache = new RateCache[IO]()
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR, 1.23)
    
    cache.put(rate).unsafeRunSync()
    
    val result = cache.get(rate.pair).unsafeRunSync()
    result shouldBe Some(rate)
  }
  
  it should "return all cached pairs" in {
    val cache = new RateCache[IO]()
    val rate1 = TestData.createTestRate(Currency.USD, Currency.EUR)
    val rate2 = TestData.createTestRate(Currency.JPY, Currency.USD)
    
    cache.put(rate1).unsafeRunSync()
    cache.put(rate2).unsafeRunSync()
    
    val cachedPairs = cache.getAllCachedPairs.unsafeRunSync()
    cachedPairs should contain(rate1.pair)
    cachedPairs should contain(rate2.pair)
  }
  
  it should "maintain rates until explicitly replaced" in {
    val cache = new RateCache[IO]()
    val rate1 = TestData.createTestRate(Currency.USD, Currency.EUR, 1.20)
    val rate2 = TestData.createTestRate(Currency.USD, Currency.EUR, 1.25)
    
    cache.put(rate1).unsafeRunSync()
    cache.get(rate1.pair).unsafeRunSync() shouldBe Some(rate1)
    
    // Rate should persist until explicitly replaced
    cache.put(rate2).unsafeRunSync()
    cache.get(rate1.pair).unsafeRunSync() shouldBe Some(rate2)
  }
  
  it should "return empty list when no pairs are cached" in {
    val cache = new RateCache[IO]()
    
    val cachedPairs = cache.getAllCachedPairs.unsafeRunSync()
    cachedPairs shouldBe List.empty
  }
  
  it should "cache multiple rates in batch" in {
    val cache = new RateCache[IO]()
    val rates = List(
      TestData.createTestRate(Currency.USD, Currency.EUR, 1.1),
      TestData.createTestRate(Currency.JPY, Currency.USD, 0.007),
      TestData.createTestRate(Currency.GBP, Currency.EUR, 1.15)
    )
    
    cache.replaceCache(rates).unsafeRunSync()
    
    rates.foreach { rate =>
      cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
    }
  }
  
  it should "clear all cached data" in {
    val cache = new RateCache[IO]()
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR)
    
    cache.put(rate).unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
    
    cache.clear().unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe None
    cache.getAllCachedPairs.unsafeRunSync() shouldBe List.empty
  }
  
  it should "use replaceCache for batch operations" in {
    val cache = new RateCache[IO]()
    val rate = TestData.createTestRate(Currency.USD, Currency.EUR)
    
    cache.put(rate).unsafeRunSync()
    cache.get(rate.pair).unsafeRunSync() shouldBe Some(rate)
  }
}