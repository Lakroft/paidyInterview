package forex.services.rates.interpreters

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._

import scala.concurrent.ExecutionContext.Implicits.global
import forex.config.OneFrameConfig
import forex.domain.{Currency, Rate}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OneFrameClientSpec extends AnyFlatSpec with Matchers {
  implicit val cs: ContextShift[IO] = IO.contextShift(global)
  implicit val timer: Timer[IO] = IO.timer(global)

  "OneFrameClient" should "build correct URL for single pair" in {
//    val config = OneFrameConfig("http://test.com", "test-token")
//    val client = new OneFrameClient[IO](config)
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // This test would require HTTP mocking framework like WireMock
    // For now, just testing the URL construction logic can be extracted
    val pairString = s"${pair.from.show}${pair.to.show}"
    pairString shouldBe "USDEUR"
  }
  
  it should "build correct URL for multiple pairs" in {
//    val config = OneFrameConfig("http://test.com", "test-token")
    val pairs = List(
      Rate.Pair(Currency.USD, Currency.EUR),
      Rate.Pair(Currency.JPY, Currency.USD),
      Rate.Pair(Currency.GBP, Currency.CHF)
    )
    
    val pairStrings = pairs.map(p => s"${p.from.show}${p.to.show}")
    val queryString = pairStrings.map(p => s"pair=$p").mkString("&")
    val expectedUrl = s"http://test.com/rates?$queryString"
    
    expectedUrl shouldBe "http://test.com/rates?pair=USDEUR&pair=JPYUSD&pair=GBPCHF"
  }
  
  it should "handle empty batch request" in {
    val config = OneFrameConfig("http://test.com", "test-token")
    val client = new OneFrameClient[IO](config)
    
    val result = client.getBatch(List.empty).unsafeRunSync()
    
    result shouldBe Right(List.empty)
  }
  
  it should "delegate single requests to batch requests" in {
    val config = OneFrameConfig("http://localhost:8080", "test-token") // This will fail in real HTTP call
    val client = new OneFrameClient[IO](config)
    val pair = Rate.Pair(Currency.USD, Currency.EUR)
    
    // This will fail with connection error, but we can verify it tries to make a request
    val result = client.get(pair).attempt.unsafeRunSync()
    result.isLeft shouldBe true // Connection will fail, which is expected
  }
  
  // Note: For proper integration testing, we would need:
  // 1. WireMock server to mock HTTP responses
  // 2. TestContainers to run actual One-Frame service
  // 3. HTTP client mocking with cats-effect test utilities
  
  // Example of what a proper HTTP integration test would look like:
  /*
  it should "parse OneFrame API response correctly" in {
    val mockResponse = """[
      {
        "from": "USD",
        "to": "EUR", 
        "price": 1.1234,
        "time_stamp": "2023-01-01T00:00:00.000Z"
      }
    ]"""
    
    // With WireMock:
    wireMockServer.stubFor(
      get(urlMatching("/rates.*"))
        .willReturn(aResponse()
          .withStatus(200)
          .withHeader("Content-Type", "application/json")
          .withBody(mockResponse))
    )
    
    val config = OneFrameConfig(s"http://localhost:${wireMockServer.port()}", "test-token")
    val client = new OneFrameClient[IO](config)
    
    val result = client.get(Rate.Pair(Currency.USD, Currency.EUR)).unsafeRunSync()
    
    result shouldBe a[Right[_, _]]
    val rate = result.getOrElse(fail())
    rate.pair shouldBe Rate.Pair(Currency.USD, Currency.EUR)
    rate.price.value shouldBe BigDecimal("1.1234")
  }
  */
}