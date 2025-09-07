package forex.services.rates.interpreters

import cats.effect.{ConcurrentEffect, Sync}
import cats.implicits.{catsSyntaxApplicativeError, toFlatMapOps}
import cats.syntax.either._
import cats.syntax.functor._
import forex.domain.{Currency, Price, Rate, Timestamp}
import forex.services.rates.Algebra
import forex.services.rates.errors.Error.{AuthenticationError, InvalidResponse, NetworkError, RateNotFound, RateLimitExceeded, ServiceUnavailable}
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

final case class OneFrameResponse(
    from: String,
    to: String,
    price: BigDecimal,
    time_stamp: String
)

class OneFrameClient[F[_]: ConcurrentEffect](config: OneFrameConfig)(implicit ec: ExecutionContext) extends Algebra[F] {

  private val logger = LoggerFactory.getLogger(classOf[OneFrameClient[F]])
  
  def checkTimeSync(timestamp: String): Unit = {
    try {
      val apiTimestamp = OffsetDateTime.parse(timestamp)
      val now = OffsetDateTime.now()
      val timeDiff = java.time.Duration.between(now, apiTimestamp)
      val absDiff = timeDiff.abs()
      
      if (absDiff.compareTo(java.time.Duration.ofSeconds(config.timeTolerance.toSeconds)) > 0) {
        val direction = if (timeDiff.isNegative) "behind" else "ahead"
        logger.warn(s"Time synchronization issue detected: API timestamp $timestamp is ${absDiff.getSeconds}s $direction of server time $now (tolerance: ${config.timeTolerance.toSeconds}s)")
      }
    } catch {
      case ex: Exception =>
        logger.warn(s"Invalid timestamp format from API: $timestamp - ${ex.getMessage}")
    }
  }
  
  def buildBatchUrl(pairs: List[Rate.Pair]): String = {
    val pairStrings = pairs.map(p => s"${p.from}${p.to}")
    val queryString = pairStrings.map(p => s"pair=$p").mkString("&")
    s"${config.url}$queryString"
  }

  override def getBatch(pairs: List[Rate.Pair]): F[Error Either List[Rate]] = {
    val logInfo = (msg: String) => Sync[F].delay(logger.info(msg))
    if (pairs.isEmpty) {
      logInfo("Empty batch request, returning empty list").flatMap { _ =>
        ConcurrentEffect[F].pure(List.empty[Rate].asRight[Error])
      }
    } else {
      val pairsStr = pairs.map(p => s"${p.from}${p.to}").mkString(", ")
      val uriString = buildBatchUrl(pairs)
      val uri = Uri.unsafeFromString(uriString)
      
      logInfo(s"Making batch HTTP request for pairs: [$pairsStr]").flatMap { _ =>
        BlazeClientBuilder[F](ec).resource.use { client =>
          val request = Request[F](
            method = Method.GET,
            uri = uri,
            headers = Headers.apply(Header.Raw.apply(name = ci"token", value = config.token))
          )

          client.expect[List[OneFrameResponse]](request).map { responses =>
            val rates = responses.flatMap { response =>
              checkTimeSync(response.time_stamp)
              for {
                fromCurrency <- Currency.fromString(response.from)
                toCurrency <- Currency.fromString(response.to)
              } yield Rate(
                Rate.Pair(fromCurrency, toCurrency),
                Price(response.price),
                Timestamp(OffsetDateTime.parse(response.time_stamp))
              )
            }
            if (responses.isEmpty) {
              logger.warn(s"Empty response from One-Frame for pairs [$pairsStr] - possibly same currency pairs or unsupported pairs")
            } else if (responses.length < pairs.length) {
              val returnedPairs = rates.map(r => s"${r.pair.from}${r.pair.to}").mkString(", ")
              logger.warn(s"Partial response from One-Frame: requested ${pairs.length} pairs [$pairsStr], received ${responses.length} rates [$returnedPairs]")
            } else {
              logger.debug(s"Batch request successful: received ${rates.length} rates")
            }
            rates.asRight[Error]
          }.handleError { ex =>

            val errorMessage = ex.getMessage
            
            ex match {
              case _: java.net.ConnectException =>
                logger.error(s"Connection failed for pairs [$pairsStr]: $errorMessage", ex)
                (NetworkError(s"Unable to connect to OneFrame service", Some(ex)): Error).asLeft[List[Rate]]
              case _: java.net.SocketTimeoutException =>
                logger.error(s"Request timeout for pairs [$pairsStr]: $errorMessage", ex)
                (NetworkError(s"Request timeout", Some(ex)): Error).asLeft[List[Rate]]
              case _ if errorMessage.contains("Forbidden") =>
                logger.error(s"Authentication failed for pairs [$pairsStr]: $errorMessage")
                (AuthenticationError("Invalid or expired token"): Error).asLeft[List[Rate]]
              case _ if errorMessage.contains("No currency pair provided") =>
                logger.error(s"Invalid request for pairs [$pairsStr]: $errorMessage")
                (InvalidResponse("No currency pair provided in request"): Error).asLeft[List[Rate]]
              case _ if errorMessage.contains("Invalid Currency Pair") =>
                logger.error(s"Invalid currency pair for pairs [$pairsStr]: $errorMessage")
                (RateNotFound(pairsStr): Error).asLeft[List[Rate]]
              case _ if errorMessage.contains("Quota reached") =>
                logger.error(s"Rate limit exceeded for pairs [$pairsStr]: $errorMessage")
                (RateLimitExceeded("Daily quota exceeded"): Error).asLeft[List[Rate]]
              case _ if errorMessage.contains("404") =>
                logger.error(s"Endpoint not found for pairs [$pairsStr]: $errorMessage")
                (ServiceUnavailable("OneFrame service endpoint not found"): Error).asLeft[List[Rate]]
              case _ if errorMessage.contains("503") =>
                logger.error(s"Service unavailable for pairs [$pairsStr]: $errorMessage")
                (ServiceUnavailable("OneFrame service temporarily unavailable"): Error).asLeft[List[Rate]]
              case _ =>
                logger.error(s"Batch request failed for pairs [$pairsStr]: $errorMessage", ex)
                (NetworkError(s"Request failed: $errorMessage", Some(ex)): Error).asLeft[List[Rate]]
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
          case None => 
            val pairStr = s"${pair.from}${pair.to}"
            logger.warn(s"No rate found in API response for pair: $pairStr")
            (RateNotFound(pairStr): Error).asLeft[Rate]
        }
      case Left(error) => error.asLeft[Rate]
    }
  }
}