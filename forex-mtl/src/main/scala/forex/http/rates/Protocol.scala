package forex.http
package rates

import forex.domain.Rate.Pair
import forex.domain._
import io.circe._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveConfiguredEncoder

object Protocol {

  implicit val configuration: Configuration = Configuration.default.withSnakeCaseMemberNames

  final case class GetApiRequest(
      from: Currency.Currency,
      to: Currency.Currency
  )

  final case class GetApiResponse(
      from: Currency.Currency,
      to: Currency.Currency,
      price: Price,
      timestamp: Timestamp
  )

  final case class ErrorApiResponse(
      error: String,
      message: String,
      timestamp: String
  )

  implicit val currencyEncoder: Encoder[Currency.Currency] =
    Encoder.instance[Currency.Currency] { c => Json.fromString(c.toString) }

  implicit val pairEncoder: Encoder[Pair] =
    deriveConfiguredEncoder[Pair]

  implicit val rateEncoder: Encoder[Rate] =
    deriveConfiguredEncoder[Rate]

  implicit val responseEncoder: Encoder[GetApiResponse] =
    deriveConfiguredEncoder[GetApiResponse]

  implicit val errorResponseEncoder: Encoder[ErrorApiResponse] =
    deriveConfiguredEncoder[ErrorApiResponse]

}
