package com.github.pwliwanow.foundationdb4s.streams

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic}
import akka.stream.{Attributes, Outlet, SourceShape}
import com.apple.foundationdb._
import com.github.pwliwanow.foundationdb4s.core.{RefreshingSubspaceStream, TypedSubspace}

import scala.concurrent.ExecutionContextExecutor

/** Factories to create sources from provided subspace.
  *
  * SubspaceSource does not guarantee to stream all data within a single transaction.
  * It can be useful when data within a subspace is immutable and append-only,
  * or if approximation is good enough.
  *
  * SubspaceSource will:
  * - complete when all elements were produced
  * - emit failure when [[KeyValue]] cannot be converted into entity
  * - fail stage if it cannot connect to the database
  */
object SubspaceSource {
  private val MaxAllowedNumberOfRestartsWithoutProgress = 3

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      database: Database): Source[Entity, NotUsed] = {
    val createStream = (ec: ExecutionContextExecutor) =>
      RefreshingSubspaceStream.fromTypedSubspace(subspace, database)(ec)
    Source.fromGraph(new SubspaceSource[Entity](createStream))
  }

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      database: Database,
      begin: KeySelector): Source[Entity, NotUsed] = {
    val createStream = (ec: ExecutionContextExecutor) =>
      RefreshingSubspaceStream.fromTypedSubspace(subspace, database, begin)(ec)
    Source.fromGraph(new SubspaceSource[Entity](createStream))
  }

  def from[Entity, KeyRepr](
      subspace: TypedSubspace[Entity, KeyRepr],
      database: Database,
      begin: KeySelector,
      end: KeySelector,
      reverse: Boolean = false,
      streamingMode: StreamingMode = StreamingMode.MEDIUM): Source[Entity, NotUsed] = {
    val createStream = (ec: ExecutionContextExecutor) =>
      RefreshingSubspaceStream.fromTypedSubspace(
        subspace,
        database,
        begin,
        end,
        reverse,
        streamingMode,
        MaxAllowedNumberOfRestartsWithoutProgress)(ec)
    Source.fromGraph(new SubspaceSource[Entity](createStream))
  }
}

private final class SubspaceSource[Entity](
    createStream: ExecutionContextExecutor => RefreshingSubspaceStream[Entity])
    extends GraphStage[SourceShape[Entity]] {
  private val out: Outlet[Entity] = Outlet("SubspaceSource.out")

  override def shape: SourceShape[Entity] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new SubspaceGraphStageLogic[Entity](shape, out, inheritedAttributes, createStream) {
      override protected def endReached(): Unit = completeStage()
    }
  }
}
