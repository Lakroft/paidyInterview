package forex.programs.rates

import forex.services.rates.errors.{ Error => RatesServiceError }

object errors {

  sealed trait Error extends Exception {
    def message: String
    def errorCode: String
    def httpStatusCode: Int
  }
  
  object Error {
    final case class RateLookupFailed(msg: String, code: String, httpStatus: Int = 500) extends Error {
      val message: String = msg
      val errorCode: String = code
      val httpStatusCode: Int = httpStatus
    }
  }

  def toProgramError(error: RatesServiceError): Error = error match {
    case RatesServiceError.OneFrameLookupFailed(msg) => 
      Error.RateLookupFailed(msg, "ONEFRAME_LOOKUP_FAILED", 500)
    case RatesServiceError.NetworkError(msg, _) => 
      Error.RateLookupFailed(msg, "NETWORK_ERROR", 502)
    case RatesServiceError.AuthenticationError(msg) => 
      Error.RateLookupFailed(msg, "AUTHENTICATION_ERROR", 500)
    case RatesServiceError.RateNotFound(pair) => 
      Error.RateLookupFailed(s"Rate not found for currency pair: $pair", "RATE_NOT_FOUND", 404)
    case RatesServiceError.InvalidResponse(msg) => 
      Error.RateLookupFailed(msg, "INVALID_RESPONSE", 502)
    case RatesServiceError.ServiceUnavailable(msg) => 
      Error.RateLookupFailed(msg, "SERVICE_UNAVAILABLE", 503)
    case RatesServiceError.RateLimitExceeded(msg) => 
      Error.RateLookupFailed(msg, "RATE_LIMIT_EXCEEDED", 429)
    case RatesServiceError.InvalidCurrencyPair(pair, reason) => 
      Error.RateLookupFailed(s"Invalid currency pair $pair: $reason", "INVALID_CURRENCY_PAIR", 400)
  }
}
