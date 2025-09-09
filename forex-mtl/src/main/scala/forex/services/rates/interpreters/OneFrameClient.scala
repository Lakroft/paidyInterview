package forex.services.rates.interpreters

import cats.effect.{ConcurrentEffect, Resource, Sync}
import cats.implicits._
import forex.domain.{Currency, Price, Rate, Timestamp}
import forex.services.rates.Algebra
import forex.services.rates.errors.Error.{AuthenticationError, InvalidResponse, NetworkError, RateNotFound, RateLimitExceeded, ServiceUnavailable}
import forex.services.rates.errors._
import io.circe.generic.auto._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
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

class OneFrameClient[F[_]: ConcurrentEffect](config: OneFrameConfig, clientResource: Resource[F, Client[F]])() extends Algebra[F] {

  private val logger = LoggerFactory.getLogger(classOf[OneFrameClient[F]])
  
  private def logWarn(msg: String): F[Unit] = Sync[F].delay(logger.warn(msg))
  private def logError(msg: String, ex: Throwable): F[Unit] = Sync[F].delay(logger.error(msg, ex))
  private def logError(msg: String): F[Unit] = Sync[F].delay(logger.error(msg))
  private def logInfo(msg: String): F[Unit] = Sync[F].delay(logger.info(msg))
  private def logDebug(msg: String): F[Unit] = Sync[F].delay(logger.debug(msg))
  
  def checkTimeSync(timestamp: String): F[Unit] = {
    Sync[F].delay {
      try {
        val apiTimestamp = OffsetDateTime.parse(timestamp)
        val now = OffsetDateTime.now()
        val timeDiff = java.time.Duration.between(now, apiTimestamp)
        val absDiff = timeDiff.abs()
        
        if (absDiff.compareTo(java.time.Duration.ofSeconds(config.timeTolerance.toSeconds)) > 0) {
          val direction = if (timeDiff.isNegative) "behind" else "ahead"
          Some(s"Time synchronization issue detected: API timestamp $timestamp is ${absDiff.getSeconds}s $direction of server time $now (tolerance: ${config.timeTolerance.toSeconds}s)")
        } else None
      } catch {
        case ex: Exception =>
          Some(s"Invalid timestamp format from API: $timestamp - ${ex.getMessage}")
      }
    }.flatMap {
      case Some(warnMsg) => logWarn(warnMsg)
      case None => Sync[F].unit
    }
  }
  
  def buildBatchUrl(pairs: List[Rate.Pair]): String = {
    val pairStrings = pairs.map(p => s"${p.from}${p.to}")
    val queryString = pairStrings.map(p => s"pair=$p").mkString("&")
    s"${config.url}$queryString"
  }

  override def getBatch(pairs: List[Rate.Pair]): F[Error Either List[Rate]] = {
    if (pairs.isEmpty) {
      logInfo("Empty batch request, returning empty list").map { _ =>
        List.empty[Rate].asRight[Error]
      }
    } else {
      val pairsStr = pairs.map(p => s"${p.from}${p.to}").mkString(", ")
      val uriString = buildBatchUrl(pairs)
      val uri = Uri.unsafeFromString(uriString)
      
      logInfo(s"Making batch HTTP request for pairs: [$pairsStr]").flatMap { _ =>
        clientResource.use { client =>
          val request = Request[F](
            method = Method.GET,
            uri = uri,
            headers = Headers.apply(Header.Raw.apply(name = ci"token", value = config.token))
          )

          client.expect[List[OneFrameResponse]](request).flatMap { responses =>
            val ratesF = responses.traverse { response =>
              checkTimeSync(response.time_stamp).as {
                for {
                  fromCurrency <- Currency.fromString(response.from)
                  toCurrency <- Currency.fromString(response.to)
                } yield Rate(
                  Rate.Pair(fromCurrency, toCurrency),
                  Price(response.price),
                  Timestamp(OffsetDateTime.parse(response.time_stamp))
                )
              }
            }.map(_.flatten)
            
            ratesF.flatMap { rates =>
              val logMessage = if (responses.isEmpty) {
                Some(s"Empty response from One-Frame for pairs [$pairsStr] - possibly same currency pairs or unsupported pairs")
              } else if (responses.length < pairs.length) {
                val returnedPairs = rates.map(r => s"${r.pair.from}${r.pair.to}").mkString(", ")
                Some(s"Partial response from One-Frame: requested ${pairs.length} pairs [$pairsStr], received ${responses.length} rates [$returnedPairs]")
              } else {
                None
              }
              
              val logF = logMessage match {
                case Some(msg) => logWarn(msg)
                case None => logDebug(s"Batch request successful: received ${rates.length} rates")
              }
              
              logF.map(_ => rates.asRight[Error])
            }
          }.handleErrorWith { ex =>
            val errorMessage = ex.getMessage
            
            val (errorF, result) = ex match {
              case _: java.net.ConnectException =>
                (logError(s"Connection failed for pairs [$pairsStr]: $errorMessage", ex),
                 (NetworkError(s"Unable to connect to OneFrame service", Some(ex)): Error).asLeft[List[Rate]])
              case _: java.net.SocketTimeoutException =>
                (logError(s"Request timeout for pairs [$pairsStr]: $errorMessage", ex),
                 (NetworkError(s"Request timeout", Some(ex)): Error).asLeft[List[Rate]])
              case _ if errorMessage.contains("Forbidden") =>
                (logError(s"Authentication failed for pairs [$pairsStr]: $errorMessage"),
                 (AuthenticationError("Invalid or expired token"): Error).asLeft[List[Rate]])
              case _ if errorMessage.contains("No currency pair provided") =>
                (logError(s"Invalid request for pairs [$pairsStr]: $errorMessage"),
                 (InvalidResponse("No currency pair provided in request"): Error).asLeft[List[Rate]])
              case _ if errorMessage.contains("Invalid Currency Pair") =>
                (logError(s"Invalid currency pair for pairs [$pairsStr]: $errorMessage"),
                 (RateNotFound(pairsStr): Error).asLeft[List[Rate]])
              case _ if errorMessage.contains("Quota reached") =>
                (logError(s"Rate limit exceeded for pairs [$pairsStr]: $errorMessage"),
                 (RateLimitExceeded("Daily quota exceeded"): Error).asLeft[List[Rate]])
              case _ if errorMessage.contains("404") =>
                (logError(s"Endpoint not found for pairs [$pairsStr]: $errorMessage"),
                 (ServiceUnavailable("OneFrame service endpoint not found"): Error).asLeft[List[Rate]])
              case _ if errorMessage.contains("503") =>
                (logError(s"Service unavailable for pairs [$pairsStr]: $errorMessage"),
                 (ServiceUnavailable("OneFrame service temporarily unavailable"): Error).asLeft[List[Rate]])
              case _ =>
                (logError(s"Batch request failed for pairs [$pairsStr]: $errorMessage", ex),
                 (NetworkError(s"Request failed: $errorMessage", Some(ex)): Error).asLeft[List[Rate]])
            }
            
            errorF.as(result)
          }
        }
      }
    }
  }

  override def get(pair: Rate.Pair): F[Error Either Rate] = {
    getBatch(List(pair)).flatMap {
      case Right(rates) => 
        rates.headOption match {
          case Some(rate) => ConcurrentEffect[F].pure(rate.asRight[Error])
          case None => 
            val pairStr = s"${pair.from}${pair.to}"
            logWarn(s"No rate found in API response for pair: $pairStr").as(
              (RateNotFound(pairStr): Error).asLeft[Rate]
            )
        }
      case Left(error) => ConcurrentEffect[F].pure(error.asLeft[Rate])
    }
  }
}

object OneFrameClient {
  def apply[F[_]: ConcurrentEffect](config: OneFrameConfig)(implicit ec: ExecutionContext): OneFrameClient[F] = {
    val clientResource = BlazeClientBuilder[F](ec).resource
    new OneFrameClient[F](config, clientResource)()
  }
}