package forex.services.rates

import cats.effect.ConcurrentEffect
import cats.syntax.functor._
import forex.config.{OneFrameConfig}
import interpreters._
import scala.concurrent.ExecutionContext

object Interpreters {
  def cachedOneFrame[F[_]: ConcurrentEffect](
      oneFrameConfig: OneFrameConfig
  )(implicit ec: ExecutionContext): F[Algebra[F]] =
    CachedOneFrame[F](oneFrameConfig).map(identity[Algebra[F]])
}
