package forex.http.rates

import forex.domain.Currency
import org.http4s.{QueryParamDecoder, ParseFailure}
import org.http4s.dsl.impl.QueryParamDecoderMatcher

object QueryParams {

  private[http] implicit val currencyQueryParam: QueryParamDecoder[Currency] =
    QueryParamDecoder[String].emap(s => 
      Currency.fromString(s).toRight(ParseFailure(s"Invalid currency: $s", s)))

  object FromQueryParam extends QueryParamDecoderMatcher[Currency]("from")
  object ToQueryParam extends QueryParamDecoderMatcher[Currency]("to")

}
