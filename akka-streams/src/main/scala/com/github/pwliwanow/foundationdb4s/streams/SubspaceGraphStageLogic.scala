package com.github.pwliwanow.foundationdb4s.streams
import akka.stream.ActorAttributes.SupervisionStrategy
import akka.stream.Supervision.Decider
import akka.stream.{Attributes, Outlet, Shape, Supervision}
import akka.stream.stage.{AsyncCallback, GraphStageLogic, OutHandler}
import com.github.pwliwanow.foundationdb4s.core.RefreshingSubspaceStream
import com.github.pwliwanow.foundationdb4s.core.internal.future.Fdb4sFastFuture.toFutureOps

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

private[streams] abstract class SubspaceGraphStageLogic[Entity](
    shape: Shape,
    out: Outlet[Entity],
    inheritedAttributes: Attributes,
    createStream: ExecutionContextExecutor => RefreshingSubspaceStream[Entity])
    extends GraphStageLogic(shape) { stage =>

  protected def endReached(): Unit

  def decider: Decider =
    inheritedAttributes
      .get[SupervisionStrategy]
      .map(_.decider)
      .getOrElse(Supervision.stoppingDecider)

  private var underlyingStream: RefreshingSubspaceStream[Entity] = _

  override def preStart(): Unit = underlyingStream = createStream(materializer.executionContext)

  override def postStop(): Unit = underlyingStream.close()

  setHandler(
    out,
    new OutHandler {
      override def onPull(): Unit = stage.onPull()
    })

  protected def resumeStream(): Unit = {
    underlyingStream.resume()
  }

  protected def onPull(): Unit = {
    val pushCallback = createPushCallback()
    val failStageCallback = createFailStageCallback()
    underlyingStream
      .onHasNext()
      .toFastFuture
      .transform {
        case Success(hasNext) => Success(pushCallback.invoke(hasNext))
        case Failure(e)       => Success(failStageCallback.invoke(e))
      }
    ()
  }

  private def createPushCallback(): AsyncCallback[Boolean] =
    getAsyncCallback[Boolean] { hasNext =>
      if (hasNext) pushNext()
      else endReached()
    }

  private def pushNext(): Unit = {
    try {
      val next = underlyingStream.next()
      push(out, next)
    } catch {
      case t: Throwable =>
        decider(t) match {
          case Supervision.Stop => failStage(t)
          case _                => onPull()
        }
    }
  }

  private def createFailStageCallback(): AsyncCallback[Throwable] =
    getAsyncCallback[Throwable](failStage)
}
