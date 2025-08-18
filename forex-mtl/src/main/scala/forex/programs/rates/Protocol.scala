package forex.programs.rates

import forex.domain.Currency

object Protocol {

  final case class GetRatesRequest(
      from: Currency.Currency,
      to: Currency.Currency
  )

}
