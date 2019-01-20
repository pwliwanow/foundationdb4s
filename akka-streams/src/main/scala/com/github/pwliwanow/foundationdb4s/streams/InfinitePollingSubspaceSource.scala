package com.github.pwliwanow.foundationdb4s.streams
import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb.{KeySelector, StreamingMode}
import com.github.pwliwanow.foundationdb4s.core.{
  RefreshingSubspaceStream,
  Transactor,
  TypedSubspace
}

import scala.concurrent.duration.FiniteDuration

/** Factories to create InfiniteSubspaceSource from provided subspace.
  *
  * InfiniteSubspaceSource does not guarantee to stream all data within a single transaction.
  * It can be useful when data within a subspace is immutable and append-only
  * and user wants to process the data once it is appended to the subspace.
  *
  * InfiniteSubspaceSource will:
  * - emit failure when [[com.apple.foundationdb.KeyValue]] cannot be converted into entity
  * - fail stage if it cannot connect to the database
  * - poll subspace after it reaches the end
  */
object InfinitePollingSubspaceSource {

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      transactor: Transactor,
      pollingInterval: FiniteDuration): Source[Entity, NotUsed] = {
    val createStream = () => RefreshingSubspaceStream.fromTypedSubspace(subspace, transactor)
    Source.fromGraph(new InfinitePollingSubspaceSource[Entity](pollingInterval, createStream))
  }

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      transactor: Transactor,
      pollingInterval: FiniteDuration,
      begin: KeySelector): Source[Entity, NotUsed] = {
    val createStream = () => RefreshingSubspaceStream.fromTypedSubspace(subspace, transactor, begin)
    Source.fromGraph(new InfinitePollingSubspaceSource[Entity](pollingInterval, createStream))
  }

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      transactor: Transactor,
      pollingInterval: FiniteDuration,
      begin: KeySelector,
      end: KeySelector,
      reverse: Boolean = false,
      streamingMode: StreamingMode = StreamingMode.MEDIUM,
      maxAllowedNumberOfRestartsWithoutProgress: Int = 3): Source[Entity, NotUsed] = {
    val createStream = () =>
      RefreshingSubspaceStream.fromTypedSubspace(
        subspace,
        transactor,
        begin,
        end,
        reverse,
        streamingMode,
        maxAllowedNumberOfRestartsWithoutProgress)
    Source.fromGraph(new InfinitePollingSubspaceSource[Entity](pollingInterval, createStream))
  }
}

private final class InfinitePollingSubspaceSource[Entity](
    pollingInterval: FiniteDuration,
    createStream: () => RefreshingSubspaceStream[Entity])
    extends GraphStage[SourceShape[Entity]] {

  private val out: Outlet[Entity] = Outlet("InfinitePollingSubspaceSource.out")

  override def shape: SourceShape[Entity] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new SubspaceGraphStageLogic[Entity](shape, out, inheritedAttributes, createStream) {
      override protected def endReached(): Unit = {
        materializer.scheduleOnce(pollingInterval, () => {
          resumeStream()
          onPull()
        })
        ()
      }
    }
  }
}
