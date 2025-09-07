package forex.services.rates

import forex.domain.Rate
import errors._

import cats.Applicative
import cats.implicits._

trait Algebra[F[_]] {
  def get(pair: Rate.Pair): F[Error Either Rate]
  
  def getBatch(pairs: List[Rate.Pair])(implicit F: Applicative[F]): F[Error Either List[Rate]] = {
    pairs.traverse(get).map { results =>
      val (errors, rates) = results.separate
      if (errors.isEmpty) rates.asRight[Error]
      else errors.head.asLeft[List[Rate]]
    }
  }
}
