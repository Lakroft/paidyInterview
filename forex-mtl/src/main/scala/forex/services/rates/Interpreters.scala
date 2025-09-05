package forex.services.rates

import cats.effect.{Clock, ConcurrentEffect}
import cats.syntax.functor._
import forex.config.{CacheConfig, OneFrameConfig}
import interpreters._
import scala.concurrent.ExecutionContext

object Interpreters {
  def cachedOneFrame[F[_]: ConcurrentEffect: Clock](
      oneFrameConfig: OneFrameConfig, 
      cacheConfig: CacheConfig
  )(implicit ec: ExecutionContext): F[Algebra[F]] = 
    CachedOneFrame[F](oneFrameConfig, cacheConfig).map(identity[Algebra[F]])
}
