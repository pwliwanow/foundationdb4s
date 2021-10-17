package com.github.pwliwanow.foundationdb4s.core

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb._
import com.github.pwliwanow.foundationdb4s.core.internal.future.Fdb4sFastFuture._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

/** RefreshingSubspaceStream does not guarantee to produce all data within a single transaction.
  *
  * It can be useful when data within a subspace is immutable and append-only,
  * or if approximation is good enough.
  *
  * User of this interface must take care of synchronization.
  */
trait RefreshingSubspaceStream[A] {
  def onHasNext(): Future[Boolean]
  def next(): A
  def resume(): Unit
  def close(): Unit
}

object RefreshingSubspaceStream {
  def fromTypedSubspace[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      database: Database)(implicit
      ec: ExecutionContextExecutor): RefreshingSubspaceStream[Entity] = {
    val begin = KeySelector.firstGreaterOrEqual(subspace.range().begin)
    fromTypedSubspace(subspace, database, begin)
  }

  def fromTypedSubspace[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      database: Database,
      begin: KeySelector)(implicit
      ec: ExecutionContextExecutor): RefreshingSubspaceStream[Entity] = {
    val end = KeySelector.firstGreaterOrEqual(subspace.range().end)
    fromTypedSubspace(subspace, database, begin, end)
  }

  def fromTypedSubspace[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      database: Database,
      begin: KeySelector,
      end: KeySelector,
      reverse: Boolean = false,
      streamingMode: StreamingMode = StreamingMode.MEDIUM,
      maxAllowedNumberOfRestartsWithoutProgress: Int = 3)(implicit
      ec: ExecutionContextExecutor): RefreshingSubspaceStream[Entity] = {
    val underlying = new SubspaceStream(
      database,
      begin,
      end,
      reverse,
      streamingMode,
      maxAllowedNumberOfRestartsWithoutProgress)
    new TypedSubspaceStream(subspace, underlying)
  }

  def fromSubspace(subspace: Subspace, database: Database)(implicit
      ec: ExecutionContextExecutor): RefreshingSubspaceStream[KeyValue] = {
    val begin = KeySelector.firstGreaterOrEqual(subspace.range().begin)
    fromSubspace(subspace, database, begin)
  }

  def fromSubspace(subspace: Subspace, database: Database, begin: KeySelector)(implicit
      ec: ExecutionContextExecutor): RefreshingSubspaceStream[KeyValue] = {
    val end = KeySelector.firstGreaterOrEqual(subspace.range().end)
    fromSubspace(database, begin, end)
  }

  def fromSubspace(
      database: Database,
      begin: KeySelector,
      end: KeySelector,
      reverse: Boolean = false,
      streamingMode: StreamingMode = StreamingMode.MEDIUM,
      maxAllowedNumberOfRestartsWithoutProgress: Int = 3)(implicit
      ec: ExecutionContextExecutor): RefreshingSubspaceStream[KeyValue] = {
    new SubspaceStream(
      database,
      begin,
      end,
      reverse,
      streamingMode,
      maxAllowedNumberOfRestartsWithoutProgress)
  }

  class TooManyFailsException(fails: Int, maxAllowedFails: Int, underlying: FDBException)
      extends RuntimeException(
        s"Too many fails occured without progress. " +
          s"Max allowed = $maxAllowedFails, got $fails fails.",
        underlying)
}

private final class TypedSubspaceStream[Entity, KeyRepr](
    typedSubspace: TypedSubspace[Entity, KeyRepr],
    underlying: SubspaceStream)
    extends RefreshingSubspaceStream[Entity] {
  override def onHasNext(): Future[Boolean] = underlying.onHasNext()
  override def next(): Entity = typedSubspace.toEntity(underlying.next())
  override def resume(): Unit = underlying.resume()
  override def close(): Unit = underlying.close()
}

private final class SubspaceStream(
    database: Database,
    begin: KeySelector,
    end: KeySelector,
    reverse: Boolean,
    streamingMode: StreamingMode,
    maxAllowedNumberOfRestartsWithoutProgress: Int = 3)(implicit ec: ExecutionContextExecutor)
    extends RefreshingSubspaceStream[KeyValue] {
  import RefreshingSubspaceStream._

  private var lastKey = Option.empty[Array[Byte]]
  private var tx = Option.empty[Transaction]
  private var asyncIterator: AsyncIterator[KeyValue] = resumedIterator()
  private var numberOfRestartsWithoutProgress = 0

  override def onHasNext(): Future[Boolean] = {
    asyncIterator
      .onHasNext()
      .toFastFuture
      .recoverWith { case e: FDBException =>
        if (numberOfRestartsWithoutProgress > maxAllowedNumberOfRestartsWithoutProgress) {
          Future.failed[Boolean](
            new TooManyFailsException(
              numberOfRestartsWithoutProgress,
              maxAllowedNumberOfRestartsWithoutProgress,
              e))
        } else {
          this.numberOfRestartsWithoutProgress += 1
          asyncIterator = resumedIterator()
          onHasNext()
        }
      }
  }

  override def next(): KeyValue = {
    val kv = asyncIterator.next()
    lastKey = Some(kv.getKey)
    numberOfRestartsWithoutProgress = 0
    kv
  }

  override def resume(): Unit = {
    asyncIterator = resumedIterator()
  }

  override def close(): Unit = {
    asyncIterator.cancel()
    closeTx()
  }

  private def resumedIterator(): AsyncIterator[KeyValue] = {
    import AsyncIterator._
    closeTx()
    val transaction = database.createTransaction(ec)
    tx = Some(transaction)
    val readTx: ReadTransaction = transaction.snapshot()
    val beginSel: KeySelector = lastKey.fold(begin)(beginSelector)
    val endSel = lastKey.fold(end)(endSelector)
    readTx
      .getRange(beginSel, endSel, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse, streamingMode)
      .iterator()
      .toScala
  }

  private def beginSelector(lastSaw: Array[Byte]): KeySelector = {
    if (!reverse) KeySelector.firstGreaterThan(lastSaw)
    else begin
  }

  private def endSelector(lastSaw: Array[Byte]): KeySelector = {
    if (!reverse) end
    else KeySelector.firstGreaterOrEqual(lastSaw)
  }

  private def closeTx(): Unit = Try(tx.foreach(_.close())).getOrElse(())
}
