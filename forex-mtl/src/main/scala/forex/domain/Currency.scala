package forex.domain

import cats.Show

object Currency extends Enumeration {
  type Currency = Value
  
  val AUD, CAD, CHF, EUR, GBP, JPY, NZD, SGD, USD = Value

  implicit val show: Show[Currency] = Show.show(_.toString)

  def fromString(s: String): Option[Currency] = {
    values.find(_.toString.equalsIgnoreCase(s.trim))
  }
  
  def allCurrencies: Set[Currency] = values.toSet
  
  def supportedPairs: List[(Currency, Currency)] = {
    val currencies = allCurrencies.toList
    for {
      from <- currencies
      to <- currencies
      if from != to
    } yield (from, to)
  }

}
