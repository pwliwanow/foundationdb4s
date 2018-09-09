package com.github.pwliwanow.foundationdb4s.streams

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.{Attributes, Outlet, SourceShape}
import akka.stream.stage.{AsyncCallback, GraphStage, GraphStageLogic, OutHandler}
import com.apple.foundationdb.{FDBException, _}
import com.apple.foundationdb.async.AsyncIterator
import com.github.pwliwanow.foundationdb4s.core.{Transactor, TypedSubspace}

import scala.compat.java8.FutureConverters._
import scala.util.Try

/** Factories to create sources from provided subspace.
  *
  * SubspaceSource does not guarantee to stream all data within a single transaction.
  * It can be useful when data within a subspace is immutable and append-only,
  * or if approximation is good enough.
  *
  * SubspaceSource will:
  * - complete when all elements were produced
  * - emit failure when [[KeyValue]] cannot be converted into entity
  * - fail if it cannot connect to the database
  */
object SubspaceSource {
  private val MaxAllowedNumberOfRestartsWithoutProgress = 3

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      transactor: Transactor): Source[Entity, NotUsed] = {
    val begin = KeySelector.firstGreaterOrEqual(subspace.range().begin)
    from(subspace, transactor, begin)
  }

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      transactor: Transactor,
      begin: KeySelector): Source[Entity, NotUsed] = {
    val end = KeySelector.firstGreaterOrEqual(subspace.range().end)
    from(subspace, transactor, begin, end)
  }

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      transactor: Transactor,
      begin: KeySelector,
      end: KeySelector,
      reverse: Boolean = false,
      streamingMode: StreamingMode = StreamingMode.MEDIUM): Source[Entity, NotUsed] = {
    Source.fromGraph(
      new SubspaceSource[Entity, KeyRepr](subspace, transactor, begin, end, reverse, streamingMode))
  }
}

final class SubspaceSource[Entity, KeyRepr](
    subspace: TypedSubspace[Entity, KeyRepr],
    transactor: Transactor,
    begin: KeySelector,
    end: KeySelector,
    reverse: Boolean,
    streamingMode: StreamingMode)
    extends GraphStage[SourceShape[Entity]] {
  import SubspaceSource.MaxAllowedNumberOfRestartsWithoutProgress

  private val out: Outlet[Entity] = Outlet("SubspaceSource.out")

  override def shape: SourceShape[Entity] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) { stage =>

      private var lastKey = Option.empty[Array[Byte]]
      private var tx = Option.empty[Transaction]
      private var asyncIterable: AsyncIterator[KeyValue] = _
      private var numberOfRestartsWithoutProgress = 0

      override def preStart(): Unit = {
        asyncIterable = resumedIterator()
      }

      setHandler(out, new OutHandler {
        override def onPull(): Unit = stage.onPull()
      })

      private def onPull(): Unit = {
        val pushCallback = createPushCallback()
        val handleFdbExceptionCallback = createHandleFdbExceptionCallback()
        val failStageCallback = createFailStageCallback()
        asyncIterable
          .onHasNext()
          .toScala
          .map(hasNext => pushCallback.invoke(hasNext))(materializer.executionContext)
          .recover {
            case e: FDBException =>
              handleFdbExceptionCallback.invoke(e)
            case t =>
              failStageCallback.invoke(t)
          }(materializer.executionContext)
        ()
      }

      private def createPushCallback(): AsyncCallback[Boolean] = getAsyncCallback[Boolean] {
        hasNext =>
          if (hasNext) {
            val tryEntity = Try {
              val kv = asyncIterable.next()
              lastKey = Some(kv.getKey)
              subspace.toEntity(kv)
            }
            tryEntity
              .map { entity =>
                push(out, entity)
                numberOfRestartsWithoutProgress = 0
              }
              .recover { case t => fail(out, t) }
              .get
          } else {
            asyncIterable.cancel()
            closeTx()
            completeStage()
          }
      }

      private def createFailStageCallback(): AsyncCallback[Throwable] =
        getAsyncCallback[Throwable] { t =>
          failStage(t)
        }

      private def createHandleFdbExceptionCallback(): AsyncCallback[FDBException] = {
        getAsyncCallback[FDBException] { e =>
          if (numberOfRestartsWithoutProgress >= MaxAllowedNumberOfRestartsWithoutProgress) {
            failStage(e)
          } else {
            numberOfRestartsWithoutProgress += 1
            asyncIterable = resumedIterator()
            onPull()
          }
        }
      }

      private def resumedIterator(): AsyncIterator[KeyValue] = {
        closeTx()
        val transaction = transactor.db.createTransaction(materializer.executionContext)
        tx = Some(transaction)
        val readTx: ReadTransaction = transaction.snapshot()
        val beginSel: KeySelector = lastKey.fold(begin)(beginSelector)
        val endSel = lastKey.fold(end)(endSelector)
        readTx
          .getRange(beginSel, endSel, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse, streamingMode)
          .iterator()
      }

      private def closeTx(): Unit = Try(tx.foreach(_.close())).getOrElse(())

      private def beginSelector(lastSaw: Array[Byte]): KeySelector = {
        if (!reverse) KeySelector.firstGreaterThan(lastSaw)
        else begin
      }

      private def endSelector(lastSaw: Array[Byte]): KeySelector = {
        if (!reverse) end
        else KeySelector.firstGreaterOrEqual(lastSaw)
      }
    }
  }
}
