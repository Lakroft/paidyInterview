package forex.helpers

import cats.effect.{Clock, Sync}
import cats.syntax.applicative._

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.FiniteDuration

class TestClock[F[_]: Sync] extends Clock[F] {
  private val currentTimeMillis = new AtomicLong(System.currentTimeMillis())
  
  override def realTime(unit: TimeUnit): F[Long] = {
    val millis = currentTimeMillis.get()
    unit match {
      case TimeUnit.MILLISECONDS => millis.pure[F]
      case TimeUnit.SECONDS => (millis / 1000).pure[F]
      case TimeUnit.MINUTES => (millis / (1000 * 60)).pure[F]
      case TimeUnit.HOURS => (millis / (1000 * 60 * 60)).pure[F]
      case TimeUnit.DAYS => (millis / (1000 * 60 * 60 * 24)).pure[F]
      case TimeUnit.NANOSECONDS => (millis * 1000000).pure[F]
      case TimeUnit.MICROSECONDS => (millis * 1000).pure[F]
    }
  }
  
  override def monotonic(unit: TimeUnit): F[Long] = realTime(unit)
  
  def advance(duration: FiniteDuration): Unit = {
    currentTimeMillis.addAndGet(duration.toMillis)
    ()
  }
  
  def setTime(timeMillis: Long): Unit = {
    currentTimeMillis.set(timeMillis)
  }
  
  def currentTime: Long = currentTimeMillis.get()
}

object TestClock {
  def apply[F[_]: Sync]: TestClock[F] = new TestClock[F]
  
  def withFixedTime[F[_]: Sync](timeMillis: Long): TestClock[F] = {
    val clock = new TestClock[F]
    clock.setTime(timeMillis)
    clock
  }
}