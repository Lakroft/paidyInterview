package forex.services.rates

import cats.effect.{Clock, ConcurrentEffect}
import forex.config.{CacheConfig, OneFrameConfig}
import interpreters._
import scala.concurrent.ExecutionContext

object Interpreters {
  def cachedOneFrame[F[_]: ConcurrentEffect: Clock](
      oneFrameConfig: OneFrameConfig, 
      cacheConfig: CacheConfig
  )(implicit ec: ExecutionContext): Algebra[F] = 
    CachedOneFrame[F](oneFrameConfig, cacheConfig)
}
