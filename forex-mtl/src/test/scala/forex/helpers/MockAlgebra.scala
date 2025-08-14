package forex.helpers

import cats.Applicative
import cats.effect.Sync
import cats.syntax.either._
import forex.domain.Rate
import forex.services.rates.Algebra
import forex.services.rates.errors.Error
import forex.services.rates.errors.Error.OneFrameLookupFailed

import scala.collection.mutable

class MockAlgebra[F[_]: Sync](testClock: Option[TestClock[F]] = None) extends Algebra[F] {
  private var _callCount = 0
  private var _batchCallCount = 0
  private val _calledPairs = mutable.ListBuffer[Rate.Pair]()
  private val _batchCalledPairs = mutable.ListBuffer[List[Rate.Pair]]()
  private var _expectedBatchPairs: Option[List[Rate.Pair]] = None
  private var _shouldFail = false
  private var _batchShouldFail = false
  
  def callCount: Int = _callCount
  def batchCallCount: Int = _batchCallCount
  def calledPairs: List[Rate.Pair] = _calledPairs.toList
  def batchCalledPairs: List[List[Rate.Pair]] = _batchCalledPairs.toList
  
  def expectBatchCall(pairs: List[Rate.Pair]): Unit = {
    _expectedBatchPairs = Some(pairs)
  }
  
  def setShouldFail(fail: Boolean): Unit = {
    _shouldFail = fail
  }
  
  def setBatchShouldFail(fail: Boolean): Unit = {
    _batchShouldFail = fail
  }
  
  def reset(): Unit = {
    _callCount = 0
    _batchCallCount = 0
    _calledPairs.clear()
    _batchCalledPairs.clear()
    _expectedBatchPairs = None
    _shouldFail = false
    _batchShouldFail = false
  }
  
  override def get(pair: Rate.Pair): F[Error Either Rate] = {
    _callCount += 1
    _calledPairs += pair
    
    if (_shouldFail) {
      Sync[F].pure(OneFrameLookupFailed("Mock failure").asLeft[Rate])
    } else {
      val rate = testClock match {
        case Some(clock) => TestData.createTestRateWithClock(pair.from, pair.to, clock)
        case None => TestData.createTestRate(pair.from, pair.to)
      }
      Sync[F].pure(rate.asRight[Error])
    }
  }
  
  override def getBatch(pairs: List[Rate.Pair])(implicit F: Applicative[F]): F[Error Either List[Rate]] = {
    _batchCallCount += 1
    _batchCalledPairs += pairs
    
    _expectedBatchPairs.foreach { expected =>
      assert(pairs.toSet == expected.toSet, s"Expected batch call with ${expected}, but got ${pairs}")
    }
    
    if (_batchShouldFail) {
      Sync[F].pure(OneFrameLookupFailed("Mock batch failure").asLeft[List[Rate]])
    } else {
      val rates = pairs.map { pair =>
        testClock match {
          case Some(clock) => TestData.createTestRateWithClock(pair.from, pair.to, clock)
          case None => TestData.createTestRate(pair.from, pair.to)
        }
      }
      Sync[F].pure(rates.asRight[Error])
    }
  }
  
  def verifyBatchCalled(): Unit = {
    assert(_batchCallCount > 0, "Expected batch call but none was made")
  }
  
  def verifyBatchNotCalled(): Unit = {
    assert(_batchCallCount == 0, s"Expected no batch calls but ${_batchCallCount} were made")
  }
  
  def verifySingleCallCount(expected: Int): Unit = {
    assert(_callCount == expected, s"Expected ${expected} single calls but got ${_callCount}")
  }
}