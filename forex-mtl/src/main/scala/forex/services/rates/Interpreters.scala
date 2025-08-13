package forex.services.rates

import cats.Applicative
import cats.effect.{Clock, ConcurrentEffect}
import interpreters._
import scala.concurrent.ExecutionContext

object Interpreters {
  def dummy[F[_]: Applicative](implicit @annotation.unused ec: ExecutionContext): Algebra[F] = 
    new OneFrameDummy[F]()
    
  def cachedOneFrame[F[_]: ConcurrentEffect: Clock](implicit ec: ExecutionContext): Algebra[F] = 
    CachedOneFrame[F]
}
