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

import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext

case class OneFrameResponse(
    from: String,
    to: String,
    price: BigDecimal,
    time_stamp: String
)

class OneFrameClient[F[_]: ConcurrentEffect](implicit ec: ExecutionContext) extends Algebra[F] {

  private val logger = LoggerFactory.getLogger(classOf[OneFrameClient[F]])

  def getBatch(pairs: List[Rate.Pair]): F[Error Either List[Rate]] = {
    val logInfo = (msg: String) => Sync[F].delay(logger.info(msg))
    if (pairs.isEmpty) {
      logInfo("Empty batch request, returning empty list").flatMap { _ =>
        ConcurrentEffect[F].pure(List.empty[Rate].asRight[Error])
      }
    } else {
      val pairsStr = pairs.map(p => s"${p.from.show}${p.to.show}").mkString(", ")
      val uri = pairs.foldLeft(Uri.unsafeFromString("http://localhost:8080/rates")) { (uri, pair) =>
        val pairString = s"${pair.from.show}${pair.to.show}"
        uri.withQueryParam("pair", pairString)
      }
      
      logInfo(s"Making batch HTTP request for pairs: [${pairsStr}]").flatMap { _ =>
        logInfo(s"Batch request URL: ${uri.toString}").flatMap { _ =>
          BlazeClientBuilder[F](ec).resource.use { client =>
        val request = Request[F](
          method = Method.GET,
          uri = uri,
          headers = Headers.apply(Header.Raw.apply(name = ci"token", value = "10dc303535874aeccc86a8251e6992f5"))
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
    val logInfo = (msg: String) => Sync[F].delay(logger.info(msg))
    val pairString = s"${pair.from.show}${pair.to.show}"
    val uri = Uri.unsafeFromString("http://localhost:8080/rates").withQueryParam("pair", pairString)
    
    logInfo(s"Making single HTTP request for pair: ${pairString}").flatMap { _ =>
      logInfo(s"Single request URL: ${uri.toString}").flatMap { _ =>
        BlazeClientBuilder[F](ec).resource.use { client =>
          val request = Request[F](
            method = Method.GET,
            uri = uri,
            headers = Headers.apply(Header.Raw.apply(name = ci"token", value = "10dc303535874aeccc86a8251e6992f5"))
          )

          client.expect[List[OneFrameResponse]](request).flatMap { rates =>
            logInfo(s"Single HTTP response received: ${rates.length} rates").flatMap { _ =>
              rates.headOption match {
                case Some(response) =>
                  val rate = Rate(
                    Rate.Pair(Currency.fromString(response.from), Currency.fromString(response.to)),
                    Price(response.price),
                    Timestamp(OffsetDateTime.parse(response.time_stamp))
                  )
                  logInfo(s"Single request successful for ${pairString}: price=${response.price}, timestamp=${response.time_stamp}").map { _ =>
                    rate.asRight[Error]
                  }
                case None =>
                  logInfo(s"Single request returned empty list for ${pairString}").map { _ =>
                    (OneFrameLookupFailed("No rate found"): Error).asLeft[Rate]
                  }
              }
            }
          }.handleError { ex =>
            logger.error(s"Request failed: ${ex.getMessage}", ex)
            (OneFrameLookupFailed("Request failed"): Error).asLeft[Rate]
          }
        }
      }
    }
  }
}