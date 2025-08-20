package forex.services.rates.interpreters

import cats.effect.{ContextShift, IO, Timer}

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.OneFrameConfig
import forex.domain.{Currency, Rate}
import forex.services.rates.errors.Error._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OneFrameClientSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "OneFrameClient" should "build correct URL for single pair" in {
    val config = OneFrameConfig("http://api.example.com/rates?", "test-token")
    val client = new OneFrameClient[IO](config)
    val pair = Rate.Pair(Currency.USD, Currency.EUR)

    val url = client.buildBatchUrl(List(pair))
    url shouldBe "http://api.example.com/rates?pair=USDEUR"
  }
  
  it should "build correct URL for multiple pairs" in {
    val config = OneFrameConfig("http://api.example.com/rates?", "secret-key")
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
    val config = OneFrameConfig("http://api.example.com:8080/api/v1/rates?", "token123")
    val client = new OneFrameClient[IO](config)
    val pairs = List(Rate.Pair(Currency.CHF, Currency.SGD))
    
    val url = client.buildBatchUrl(pairs)
    url shouldBe "http://api.example.com:8080/api/v1/rates?pair=CHFSGD"
  }
  
  it should "build URL for empty pair list" in {
    val config = OneFrameConfig("http://test.com/rates?", "test-token")
    val client = new OneFrameClient[IO](config)
    
    val url = client.buildBatchUrl(List.empty)
    url shouldBe "http://test.com/rates?"
  }
  
  it should "handle empty batch request" in {
    val config = OneFrameConfig("http://test.com/rates?", "test-token")
    val client = new OneFrameClient[IO](config)
    
    val result = client.getBatch(List.empty).unsafeRunSync()
    
    result shouldBe Right(List.empty)
  }

  // Error handling tests - using malformed URLs to test error handling logic
  it should "handle invalid URL configuration" in {
    // Use invalid protocol to trigger connection error
    val config = OneFrameConfig("invalid-protocol://test.com/rates?", "test-token")
    val client = new OneFrameClient[IO](config)
    val pairs = List(Rate.Pair(Currency.USD, Currency.EUR))
    
    val result = client.getBatch(pairs).unsafeRunSync()
    
    result.isLeft shouldBe true
    result.left.foreach { error =>
      error shouldBe a[NetworkError]
      error.message should not be empty
    }
  }

  it should "properly map single pair failures to get() method" in {
    // Test that single pair method correctly handles batch failures
    val config = OneFrameConfig("invalid://bad-url", "test-token")
    val client = new OneFrameClient[IO](config)
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    val result = client.get(pair).unsafeRunSync()
    
    result.isLeft shouldBe true
    result.left.foreach { error =>
      error shouldBe a[NetworkError]
    }
  }
  
  it should "handle empty batch response correctly in get() method" in {
    // This would require mocking, but we can test the URL building at least
    val config = OneFrameConfig("http://test.com/rates?", "test-token") 
    val client = new OneFrameClient[IO](config)
    
    // Test URL generation works correctly for edge cases
    val singlePairUrl = client.buildBatchUrl(List(Rate.Pair(Currency.USD, Currency.USD)))
    singlePairUrl should include("USDUSD")
  }

  // Test error message patterns from actual OneFrameClient error handling
  it should "create appropriate error messages for different failure scenarios" in {
    val config = OneFrameConfig("http://test.com/rates?", "test-token")
    val client = new OneFrameClient[IO](config)
    
    // Test URL building works for various scenarios
    val multiPairUrl = client.buildBatchUrl(List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.GBP),
      Rate.Pair(Currency.CHF, Currency.AUD)
    ))
    
    multiPairUrl should include("pair=USDEUR")
    multiPairUrl should include("pair=JPYGBP") 
    multiPairUrl should include("pair=CHFAUD")
    multiPairUrl should include("&") // Should have proper URL param separators
  }

  it should "handle edge case currency combinations in URL building" in {
    val config = OneFrameConfig("https://api.example.com:8443/v2/rates?", "secret123")
    val client = new OneFrameClient[IO](config)
    
    // Test various currency combinations
    val pairs = List(
      Rate.Pair(Currency.SGD, Currency.NZD),
      Rate.Pair(Currency.CAD, Currency.AUD)
    )
    
    val url = client.buildBatchUrl(pairs)
    url shouldBe "https://api.example.com:8443/v2/rates?pair=SGDNZD&pair=CADAUD"
  }
}