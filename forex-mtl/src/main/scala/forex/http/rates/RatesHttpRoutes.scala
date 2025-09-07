package forex.http
package rates

import cats.effect.Sync
import cats.syntax.flatMap._
import forex.domain.Currency
import forex.programs.RatesProgram
import forex.programs.rates.{Protocol => RatesProgramProtocol}
import forex.programs.rates.errors.Error
import org.http4s.{HttpRoutes, Status}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.Router
import org.slf4j.LoggerFactory

import java.time.Instant

class RatesHttpRoutes[F[_]: Sync](rates: RatesProgram[F]) extends Http4sDsl[F] {

  import Converters._, QueryParams._, Protocol._
  
  private val logger = LoggerFactory.getLogger(classOf[RatesHttpRoutes[F]])

  private[http] val prefixPath = "/rates"

  private val httpRoutes: HttpRoutes[F] = HttpRoutes.of[F] {
    case GET -> Root :? FromQueryParam(from) +& ToQueryParam(to) =>
      rates.get(RatesProgramProtocol.GetRatesRequest(from, to)).flatMap {
        case Right(rate) => 
          Ok(rate.asGetApiResponse)
        case Left(error: Error) =>
          Sync[F].delay(logger.warn(s"API request failed: GET /rates?from=$from&to=$to - ${error.message}")).flatMap { _ =>
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
    case req @ GET -> Root if req.uri.query.nonEmpty =>
      Sync[F].delay(logger.warn(s"Invalid currency parameters in request: ${req.uri.query}")).flatMap { _ =>
        val errorResponse = ErrorApiResponse(
          error = "INVALID_PARAMETERS",
          message = "Invalid currency parameters. Supported currencies: " + Currency.allCurrencies.mkString(", "),
          timestamp = Instant.now().toString
        )
        BadRequest(errorResponse)
      }
    case GET -> Root =>
      Sync[F].delay(logger.warn("Missing required parameters 'from' and 'to' in /rates request")).flatMap { _ =>
        val errorResponse = ErrorApiResponse(
          error = "MISSING_PARAMETERS",
          message = "Missing required parameters: 'from' and 'to'",
          timestamp = Instant.now().toString
        )
        BadRequest(errorResponse)
      }
  }

  val routes: HttpRoutes[F] = Router(
    prefixPath -> httpRoutes
  )

}
