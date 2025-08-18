package forex.services.rates.interpreters

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.OneFrameConfig
import forex.domain.{Currency, Rate}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OneFrameClientSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "OneFrameClient" should "build correct URL for single pair" in {
    val config = OneFrameConfig("http://api.example.com", "test-token")
    val client = new OneFrameClient[IO](config)
    val pair = Rate.Pair(Currency.USD, Currency.EUR)

    val url = client.buildBatchUrl(List(pair))
    url shouldBe "http://api.example.com/rates?pair=USDEUR"
  }
  
  it should "build correct URL for multiple pairs" in {
    val config = OneFrameConfig("http://api.example.com", "secret-key")
    val client = new OneFrameClient[IO](config)
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    val url = client.buildBatchUrl(pairs)
    url shouldBe "http://api.example.com/rates?pair=USDEUR&pair=JPYUSD&pair=GBPCHF"
  }
  
  it should "handle special characters in base URL" in {
    val config = OneFrameConfig("http://api.example.com:8080/api/v1", "token123")
    val client = new OneFrameClient[IO](config)
    val pairs = List(Rate.Pair(Currency.CHF, Currency.SGD))
    
    val url = client.buildBatchUrl(pairs)
    url shouldBe "http://api.example.com:8080/api/v1/rates?pair=CHFSGD"
  }
  
  it should "build URL for empty pair list" in {
    val config = OneFrameConfig("http://test.com", "test-token")
    val client = new OneFrameClient[IO](config)
    
    val url = client.buildBatchUrl(List.empty)
    url shouldBe "http://test.com/rates?"
  }
  
  it should "handle empty batch request" in {
    val config = OneFrameConfig("http://test.com", "test-token")
    val client = new OneFrameClient[IO](config)
    
    val result = client.getBatch(List.empty).unsafeRunSync()
    
    result shouldBe Right(List.empty)
  }
}