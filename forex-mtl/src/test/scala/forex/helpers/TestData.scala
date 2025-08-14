package forex.helpers

import forex.domain.{Currency, Price, Rate, Timestamp}
import java.time.{Instant, OffsetDateTime}
import scala.concurrent.duration._

object TestData {
  
  def createTestRate(from: Currency, to: Currency, price: BigDecimal = 1.0): Rate = {
    Rate(
      Rate.Pair(from, to),
      Price(price),
      Timestamp(OffsetDateTime.now(java.time.ZoneOffset.UTC))
    )
  }
  
  def createTestRateWithClock[F[_]](from: Currency, to: Currency, testClock: TestClock[F], price: BigDecimal = 1.0): Rate = {
    val timestamp = Instant.ofEpochMilli(testClock.currentTime).atOffset(java.time.ZoneOffset.UTC)
    Rate(
      Rate.Pair(from, to),
      Price(price),
      Timestamp(timestamp)
    )
  }
  
  def createExpiredRate(from: Currency, to: Currency, price: BigDecimal = 1.0): Rate = {
    Rate(
      Rate.Pair(from, to),
      Price(price),
      Timestamp(OffsetDateTime.now(java.time.ZoneOffset.UTC).minusMinutes(10))
    )
  }
  
  val testPairs = List(
    Rate.Pair(Currency.USD, Currency.EUR),
    Rate.Pair(Currency.EUR, Currency.JPY),
    Rate.Pair(Currency.JPY, Currency.USD),
    Rate.Pair(Currency.GBP, Currency.USD),
    Rate.Pair(Currency.CHF, Currency.SGD)
  )
  
  val defaultTestConfig = forex.config.ApplicationConfig(
    http = forex.config.HttpConfig("localhost", 8085, 30.seconds),
    oneFrame = forex.config.OneFrameConfig("http://localhost:8080", "test-token"),
    cache = forex.config.CacheConfig(5.minutes)
  )
}