package forex.http
package rates

import cats.effect.Sync
import cats.syntax.flatMap._
import forex.programs.RatesProgram
import forex.programs.rates.{ Protocol => RatesProgramProtocol }
import forex.programs.rates.errors.Error
import org.http4s.{HttpRoutes, Status}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import java.time.Instant

class RatesHttpRoutes[F[_]: Sync](rates: RatesProgram[F]) extends Http4sDsl[F] {

  import Converters._, QueryParams._, Protocol._

  private[http] val prefixPath = "/rates"

  private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root :? FromQueryParam(from) +& ToQueryParam(to) =>
      rates.get(RatesProgramProtocol.GetRatesRequest(from, to)).flatMap {
        case Right(rate) => 
          Ok(rate.asGetApiResponse)
        case Left(error: Error) =>
          val errorResponse = ErrorApiResponse(
            error = error.errorCode,
            message = error.message,
            timestamp = Instant.now().toString
          )
          Status.fromInt(error.httpStatusCode) match {
            case Right(status) => 
              Sync[F].pure(org.http4s.Response[F](status).withEntity(errorResponse))
            case Left(_) => 
              InternalServerError(errorResponse)
          }
      }
  }

  val routes: HttpRoutes[F] = Router(
    prefixPath -> httpRoutes
  )

}
