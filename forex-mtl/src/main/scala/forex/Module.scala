package forex

import cats.effect.{Clock, ConcurrentEffect, Timer}
import forex.config.ApplicationConfig
import forex.http.rates.RatesHttpRoutes
import forex.services._
import forex.programs._
import org.http4s._
import org.http4s.implicits._
import org.http4s.server.middleware.{AutoSlash, Logger, Timeout}
import scala.concurrent.ExecutionContext

class Module[F[_]: Timer: ConcurrentEffect: Clock](config: ApplicationConfig)(implicit ec: ExecutionContext) {

  private val ratesServiceF: F[RatesService[F]] = RatesServices.cachedOneFrame[F](config.oneFrame, config.cache)
  
  // For simplicity, we'll use unsafeRunSync here since Module is initialized once at startup
  private val ratesService: RatesService[F] = {
    import cats.effect.IO
    ratesServiceF.asInstanceOf[IO[RatesService[F]]].unsafeRunSync()
  }

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

}
