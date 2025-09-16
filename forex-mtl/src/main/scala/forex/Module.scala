package forex

import cats.effect.{ConcurrentEffect, Timer}
import forex.config.ApplicationConfig
import forex.http.rates.RatesHttpRoutes
import forex.services._
import forex.services.rates.interpreters.CachedOneFrame
import forex.programs._
import org.http4s._
import org.http4s.implicits._
import org.http4s.server.middleware.{AutoSlash, Logger, Timeout}
import org.slf4j.LoggerFactory

class Module[F[_]: Timer: ConcurrentEffect](config: ApplicationConfig, cachedOneFrameInstance: CachedOneFrame[F])() {

  private val logger = LoggerFactory.getLogger(classOf[Module[F]])
  
  // Use the provided CachedOneFrame instance (created with proper resource management)
  private val cachedOneFrame = cachedOneFrameInstance
  
  // Use the same instance as RatesService
  private val ratesService: RatesService[F] = cachedOneFrame

  private val ratesProgram: RatesProgram[F] = RatesProgram[F](ratesService)

  private val ratesHttpRoutes: HttpRoutes[F] = new RatesHttpRoutes[F](ratesProgram).routes

  type PartialMiddleware = HttpRoutes[F] => HttpRoutes[F]
  type TotalMiddleware   = HttpApp[F] => HttpApp[F]

  private val routesMiddleware: PartialMiddleware = {
    { http: HttpRoutes[F] =>
      AutoSlash(http)
    }
  }

  private val appMiddleware: TotalMiddleware = { http: HttpApp[F] =>
    Logger.httpApp(logHeaders = true, logBody = false)(Timeout(config.http.timeout)(http))
  }

  private val http: HttpRoutes[F] = ratesHttpRoutes

  val httpApp: HttpApp[F] = appMiddleware(routesMiddleware(http).orNotFound)

  def updateCache(): F[Unit] = {
    import cats.implicits._
    
    for {
      _ <- ConcurrentEffect[F].delay(logger.info("Timer-based cache update started"))
      result <- cachedOneFrame.refreshCache()
      _ <- result match {
        case Right(rates) =>
          ConcurrentEffect[F].delay(logger.info(s"Timer-based cache update successful: ${rates.length} rates refreshed"))
        case Left(error) =>
          ConcurrentEffect[F].delay(logger.error(s"Timer-based cache update failed: ${error.message}"))
      }
    } yield ()
  }

}
