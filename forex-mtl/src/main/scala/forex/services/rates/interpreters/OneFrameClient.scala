package forex.services.rates.interpreters

import cats.effect.{ConcurrentEffect, Sync}
import cats.implicits.{catsSyntaxApplicativeError, toFlatMapOps, toShow}
import cats.syntax.either._
import cats.syntax.functor._
import forex.domain.{Currency, Price, Rate, Timestamp}
import forex.services.rates.Algebra
import forex.services.rates.errors.Error.OneFrameLookupFailed
import forex.services.rates.errors._
import io.circe.generic.auto._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.slf4j.LoggerFactory
import org.typelevel.ci._

import forex.config.OneFrameConfig
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

case class OneFrameResponse(
    from: String,
    to: String,
    price: BigDecimal,
    time_stamp: String
)

class OneFrameClient[F[_]: ConcurrentEffect](config: OneFrameConfig)(implicit ec: ExecutionContext) extends Algebra[F] {

  private val logger = LoggerFactory.getLogger(classOf[OneFrameClient[F]])

  def getBatch(pairs: List[Rate.Pair]): F[Error Either List[Rate]] = {
    val logInfo = (msg: String) => Sync[F].delay(logger.info(msg))
    if (pairs.isEmpty) {
      logInfo("Empty batch request, returning empty list").flatMap { _ =>
        ConcurrentEffect[F].pure(List.empty[Rate].asRight[Error])
      }
    } else {
      val pairStrings = pairs.map(p => s"${p.from.show}${p.to.show}")
      val pairsStr = pairStrings.mkString(", ")
      
      val queryString = pairStrings.map(p => s"pair=$p").mkString("&")
      val uriString = s"${config.url}/rates?$queryString"
      val uri = Uri.unsafeFromString(uriString)
      
      logInfo(s"Making batch HTTP request for pairs: [${pairsStr}]").flatMap { _ =>
        logInfo(s"Batch request URL: ${uri.toString}").flatMap { _ =>
          BlazeClientBuilder[F](ec).resource.use { client =>
            val request = Request[F](
              method = Method.GET,
              uri = uri,
              headers = Headers.apply(Header.Raw.apply(name = ci"token", value = config.token))
            )

            client.expect[List[OneFrameResponse]](request).flatMap { responses =>
              logInfo(s"Batch HTTP response received: ${responses.length} rates").map { _ =>
                val rates = responses.map { response =>
                  Rate(
                    Rate.Pair(Currency.fromString(response.from), Currency.fromString(response.to)),
                    Price(response.price),
                    Timestamp(OffsetDateTime.parse(response.time_stamp))
                  )
                }
                rates.asRight[Error]
              }
            }.handleError { ex =>
              logger.error(s"Batch request failed: ${ex.getMessage}", ex)
              (OneFrameLookupFailed("Request failed"): Error).asLeft[List[Rate]]
            }
          }
        }
      }
    }
  }

  override def get(pair: Rate.Pair): F[Error Either Rate] = {
    getBatch(List(pair)).map {
      case Right(rates) => 
        rates.headOption match {
          case Some(rate) => rate.asRight[Error]
          case None => (OneFrameLookupFailed("No rate found"): Error).asLeft[Rate]
        }
      case Left(error) => error.asLeft[Rate]
    }
  }
}