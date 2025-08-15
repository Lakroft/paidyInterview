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
    case req @ GET -> Root if req.uri.query.nonEmpty =>
      val errorResponse = ErrorApiResponse(
        error = "INVALID_PARAMETERS",
        message = "Invalid currency parameters. Supported currencies: AUD, CAD, CHF, EUR, GBP, NZD, JPY, SGD, USD",
        timestamp = Instant.now().toString
      )
      BadRequest(errorResponse)
    case GET -> Root =>
      val errorResponse = ErrorApiResponse(
        error = "MISSING_PARAMETERS",
        message = "Missing required parameters: 'from' and 'to'",
        timestamp = Instant.now().toString
      )
      BadRequest(errorResponse)
  }

  val routes: HttpRoutes[F] = Router(
    prefixPath -> httpRoutes
  )

}
