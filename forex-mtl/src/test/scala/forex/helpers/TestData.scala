package forex.helpers

import forex.domain.{Currency, Price, Rate, Timestamp}
import java.time.{Instant, OffsetDateTime}
import scala.concurrent.duration._

object TestData {
  
  def createTestRate(from: Currency.Currency, to: Currency.Currency, price: BigDecimal = 1.0): Rate = {
    Rate(
      Rate.Pair(from, to),
      Price(price),
      Timestamp(OffsetDateTime.now(java.time.ZoneOffset.UTC))
    )
  }
  
  def createTestRateWithClock[F[_]](from: Currency.Currency, to: Currency.Currency, testClock: TestClock[F], price: BigDecimal = 1.0): Rate = {
    val timestamp = Instant.ofEpochMilli(testClock.currentTime).atOffset(java.time.ZoneOffset.UTC)
    Rate(
      Rate.Pair(from, to),
      Price(price),
      Timestamp(timestamp)
    )
  }
  
  def createExpiredRate(from: Currency.Currency, to: Currency.Currency, price: BigDecimal = 1.0): Rate = {
    Rate(
      Rate.Pair(from, to),
      Price(price),
      Timestamp(OffsetDateTime.now(java.time.ZoneOffset.UTC).minusMinutes(10))
    )
  }
  
  val testPairs = Currency.supportedPairs.take(5).map { case (from, to) => Rate.Pair(from, to) }
  
  val defaultTestConfig = forex.config.ApplicationConfig(
    http = forex.config.HttpConfig("localhost", 8085, 30.seconds),
    oneFrame = forex.config.OneFrameConfig("http://localhost:8080", "test-token", 30.seconds),
    cache = forex.config.CacheConfig(5.minutes)
  )
}