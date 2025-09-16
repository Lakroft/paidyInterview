package forex.services.rates

import cats.effect.{ConcurrentEffect, Resource}
import forex.config.{OneFrameConfig}
import interpreters._
import scala.concurrent.ExecutionContext

object Interpreters {
  def cachedOneFrame[F[_]: ConcurrentEffect](
      oneFrameConfig: OneFrameConfig
  )(implicit ec: ExecutionContext): Resource[F, CachedOneFrame[F]] =
    CachedOneFrame[F](oneFrameConfig)
    
  def cachedOneFrameAlgebra[F[_]: ConcurrentEffect](
      oneFrameConfig: OneFrameConfig
  )(implicit ec: ExecutionContext): Resource[F, Algebra[F]] =
    CachedOneFrame[F](oneFrameConfig).map(identity[Algebra[F]])
}
