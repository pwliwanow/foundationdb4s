package com.github.pwliwanow.foundationdb4s.streams

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorAttributes, ActorMaterializer, Supervision}
import com.github.pwliwanow.foundationdb4s.core._
import org.scalamock.scalatest.MockFactory

import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Try}

class SubspaceSourceSpec extends FoundationDbStreamsSpec with MockFactory {

  private val entity =
    FriendEntity(
      ofUserId = 1L,
      addedAt = Instant.parse("2018-08-03T10:15:30.00Z"),
      friendId = 10L,
      friendName = "John")

  private implicit lazy val mat: ActorMaterializer = ActorMaterializer()

  it should "stream data from whole underlying RefreshingSubspaceStream" in {
    val xs = (1 to 100).iterator.map(entityFromInt).toList
    val (stream, noTimesCloseCalled) = refreshingStream(xs)
    val res = Source.fromGraph(new SubspaceSource(() => stream)).runWith(Sink.seq).awaitInf.toList
    assert(res === xs)
    assert(noTimesCloseCalled.get() === 1)
  }

  it should "fail the stream when hasNext of underlying RefreshingSubspaceStream failed" in {
    val xs = (1 to 100).iterator.map(entityFromInt).toList
    val (stream, noTimesCloseCalled) =
      refreshingStream(xs, () => Future.failed(new RuntimeException("Error occurred")))
    val res = Try(Source.fromGraph(new SubspaceSource(() => stream)).runWith(Sink.ignore).awaitInf)
    assert(res.isFailure)
    assert(res.asInstanceOf[Failure[_]].exception.getMessage === "Error occurred")
    assert(noTimesCloseCalled.get() === 1)
  }

  it should "emit failure if underlying stream threw exception for `next` method" in {
    val xs = (1 to 10).iterator.map(entityFromInt).toList
    def getElem(xs: Seq[FriendEntity], i: Int): FriendEntity = {
      if (i >= 4 && i <= 6) throw new RuntimeException("Error during decoding")
      else xs(i)
    }
    val (stream, noTimesCloseCalled) = refreshingStream(xs, getElem = getElem)
    val res =
      Source
        .fromGraph(new SubspaceSource(() => stream))
        .withAttributes(ActorAttributes.supervisionStrategy(Supervision.resumingDecider))
        .runWith(Sink.fold(ListBuffer.empty[FriendEntity])(_ += _))
        .awaitInf
    assert(res.toList === xs.take(4) ++ xs.drop(7))
    assert(noTimesCloseCalled.get() === 1)
  }

  private def refreshingStream[A](xs: Seq[A]): (RefreshingSubspaceStream[A], AtomicInteger) = {
    refreshingStream(xs, () => Future.successful(false), (xs, i) => xs(i))
  }

  private def refreshingStream[A](
      xs: Seq[A],
      emitAtTheEnd: () => Future[Boolean]): (RefreshingSubspaceStream[A], AtomicInteger) = {
    refreshingStream(xs, emitAtTheEnd, (xs, i) => xs(i))
  }

  private def refreshingStream[A](
      xs: Seq[A],
      emitAtTheEnd: () => Future[Boolean] = () => Future.successful(false),
      getElem: (Seq[A], Int) => A): (RefreshingSubspaceStream[A], AtomicInteger) = {
    val counter = new AtomicInteger(0)
    val numberOfTimesOnClosedWasCalled = new AtomicInteger(0)
    val stream = new RefreshingSubspaceStream[A] {
      override def onHasNext(): Future[Boolean] = {
        if (counter.get() < xs.length) Future.successful[Boolean](true)
        else emitAtTheEnd()
      }
      override def next(): A = getElem(xs, counter.getAndIncrement())
      override def resume(): Unit = ()
      override def close(): Unit = {
        numberOfTimesOnClosedWasCalled.incrementAndGet()
        ()
      }
    }
    (stream, numberOfTimesOnClosedWasCalled)
  }

  private def entityFromInt(i: Int): FriendEntity = {
    entity.copy(addedAt = entity.addedAt.plusSeconds(i.toLong), friendId = entity.friendId + i)
  }

}
