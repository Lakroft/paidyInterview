package forex.services.rates

object errors {

  sealed trait Error {
    def message: String
    def errorCode: String
  }
  
  object Error {
    final case class OneFrameLookupFailed(msg: String) extends Error {
      val message: String = msg
      val errorCode: String = "ONEFRAME_LOOKUP_FAILED"
    }
    
    final case class NetworkError(msg: String, cause: Option[Throwable] = None) extends Error {
      val message: String = s"Network error: $msg"
      val errorCode: String = "NETWORK_ERROR"
    }
    
    final case class AuthenticationError(msg: String) extends Error {
      val message: String = s"Authentication failed: $msg"
      val errorCode: String = "AUTHENTICATION_ERROR"  
    }
    
    final case class RateNotFound(pair: String) extends Error {
      val message: String = s"Rate not found for currency pair: $pair"
      val errorCode: String = "RATE_NOT_FOUND"
    }
    
    final case class InvalidResponse(msg: String) extends Error {
      val message: String = s"Invalid response from provider: $msg"
      val errorCode: String = "INVALID_RESPONSE"
    }
    
    final case class ServiceUnavailable(msg: String) extends Error {
      val message: String = s"External service unavailable: $msg"  
      val errorCode: String = "SERVICE_UNAVAILABLE"
    }
    
    final case class RateLimitExceeded(msg: String) extends Error {
      val message: String = s"Rate limit exceeded: $msg"
      val errorCode: String = "RATE_LIMIT_EXCEEDED"
    }
    
    final case class InvalidCurrencyPair(pair: String, reason: String) extends Error {
      val message: String = s"Invalid currency pair $pair: $reason"
      val errorCode: String = "INVALID_CURRENCY_PAIR"
    }
  }

}
