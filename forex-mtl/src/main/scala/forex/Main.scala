package forex

import scala.concurrent.ExecutionContext
import cats.effect._
import forex.config._
import forex.services._
import fs2.Stream
import org.http4s.blaze.server.BlazeServerBuilder

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    new Application[IO].stream(executionContext).compile.drain.as(ExitCode.Success)

}

class Application[F[_]: ConcurrentEffect: Timer] {

  def stream(ec: ExecutionContext): Stream[F, Unit] = {
    implicit val implicitEc: ExecutionContext = ec
    for {
      config <- Config.stream("app")
      cachedOneFrame <- Stream.resource(RatesServices.cachedOneFrame[F](config.oneFrame))
      module = new Module[F](config, cachedOneFrame)()
      // Initialize cache on startup
      _ <- Stream.eval(module.updateCache())
      server = BlazeServerBuilder[F](ec)
            .bindHttp(config.http.port, config.http.host)
            .withHttpApp(module.httpApp)
            .serve
      cacheUpdater = Stream.awakeEvery[F](config.cache.ttl).evalMap { _ =>
        module.updateCache()
      }
      _ <- server.concurrently(cacheUpdater)
    } yield ()
  }

}
