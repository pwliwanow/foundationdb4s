package com.github.pwliwanow.foundationdb4s.core
import java.time.Instant
import java.util.concurrent.{CompletableFuture, Executor}
import java.{lang, util}

import com.apple.foundationdb._
import com.apple.foundationdb.async.{AsyncIterable, AsyncIterator => FdbAsyncIterator}
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.core.RefreshingSubspaceStream.TooManyFailsException
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable.Seq
import scala.collection.mutable.ListBuffer
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration._
import scala.concurrent.{Future, blocking}
import scala.util.{Failure, Try}

class RefreshingSubspaceStreamSpec
    extends FoundationDbSpec
    with MockFactory
    with BeforeAndAfterAll {

  private val entity =
    FriendEntity(
      ofUserId = 1L,
      addedAt = Instant.parse("2018-08-03T10:15:30.00Z"),
      friendId = 10L,
      friendName = "John")

  private val earlierSubspace = new Subspace(Tuple.from("foundationDbEarlierTestSubspace"))
  private val laterSubspace = new Subspace(Tuple.from("foundationDbTestSubspaceLater"))
  private val slowDownEachIterationFor = 100.millis

  override def beforeAll(): Unit = {
    super.beforeAll()
    database.run { tx =>
      val key = Tuple.from("01", "something").pack
      val value = Tuple.from("some value").pack
      tx.set(earlierSubspace.pack(key), value)
      tx.set(laterSubspace.pack(key), value)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    database.run { tx =>
      tx.clear(earlierSubspace.range())
      tx.clear(laterSubspace.range())
    }
  }

  it should "stream data from whole subspace for streaming that last over 5s" in {
    val xs = (1 to 100).iterator.map(entityFromInt).toList
    addElements(xs)
    val res =
      collectAllAndCloseStream(
        RefreshingSubspaceStream.fromTypedSubspace(typedSubspace, slowedDownDatabase))
    assert(res === xs)
  }

  it should "be possible to resume stream once it reached the end earlier" in {
    val numberOfElements = 100
    val (firstHalf, secondHalf) =
      (1 to numberOfElements).iterator.map(entityFromInt).toList.splitAt(numberOfElements / 2)
    addElements(firstHalf)
    val stream = RefreshingSubspaceStream.fromTypedSubspace(typedSubspace, database)
    val res1 = collectAll(stream)
    assert(res1 === firstHalf)
    addElements(secondHalf)
    stream.resume()
    val res2 = collectAll(stream)
    assert(res2 === secondHalf)
    stream.close()
  }

  it should "stream data in the reverse order for streaming that last over 5s" in {
    val xs = (1 to 100).iterator.map(entityFromInt).toList
    addElements(xs)
    val begin = KeySelector.firstGreaterOrEqual(subspace.range().begin)
    val end = KeySelector.firstGreaterOrEqual(subspace.range().end)
    val res = collectAllAndCloseStream(
      RefreshingSubspaceStream.fromTypedSubspace(
        typedSubspace,
        slowedDownDatabase,
        begin = begin,
        end = end,
        reverse = true))
    assert(res === xs.reverse)
  }

  it should "stream data from" in {
    val xs = (1 to 100).iterator.map(entityFromInt).toList
    addElements(xs)
    val begin = KeySelector.firstGreaterOrEqual(subspace.range().begin)
    val end = KeySelector.firstGreaterOrEqual(subspace.range().end)
    val res = collectAllAndCloseStream(
      RefreshingSubspaceStream.fromTypedSubspace(
        typedSubspace,
        slowedDownDatabase,
        begin = begin,
        end = end,
        reverse = true))
    assert(res === xs.reverse)
  }

  it should "fail if it there was another exception than FDBException during streaming" in {
    val allEntities = (1 to 1000).iterator.map(entityFromInt).toList
    val stubbedDb = stub[Database]
    val stubbedTx = stub[Transaction]
    val mockedReadTx = mock[ReadTransaction]
    val iterable1 = stub[AsyncIterable[KeyValue]]
    val iterable2 = stub[AsyncIterable[KeyValue]]
    val exception = TestError("Unexpected error")
    val iterator =
      asyncIteratorFailedAtTheEnd(
        xs = allEntities.take(100).map(entityToKeyValue),
        exception = exception)
    (stubbedDb.createTransaction(_: Executor)).when(*).returns(stubbedTx)
    (stubbedTx.snapshot _).when().returns(mockedReadTx)
    (mockedReadTx
      .getRange(_: KeySelector, _: KeySelector, _: Int, _: Boolean, _: StreamingMode))
      .expects(*, *, *, *, *)
      .returning(iterable1)
      .once()
    (mockedReadTx
      .getRange(_: KeySelector, _: KeySelector, _: Int, _: Boolean, _: StreamingMode))
      .expects(*, *, *, *, *)
      .returning(iterable2)
      .never()
    (iterable1.iterator _).when().returns(iterator).once()
    val res = Try(
      collectAllAndCloseStream(
        RefreshingSubspaceStream.fromTypedSubspace(typedSubspace, stubbedDb)))
    assert(res === Failure(exception))
  }

  it should "fail if database keeps disconnecting" in {
    val allEntities = (1 to 1000).iterator.map(entityFromInt).toList
    val stubbedDb = stub[Database]
    val stubbedTx = stub[Transaction]
    val mockedReadTx = mock[ReadTransaction]
    val iterable = stub[AsyncIterable[KeyValue]]
    val iterator = asyncIteratorFailedAtTheEnd(allEntities.take(100).map(entityToKeyValue))
    (stubbedDb.createTransaction(_: Executor)).when(*).returns(stubbedTx)
    (stubbedTx.snapshot _).when().returns(mockedReadTx)
    (mockedReadTx
      .getRange(_: KeySelector, _: KeySelector, _: Int, _: Boolean, _: StreamingMode))
      .expects(*, *, *, *, *)
      .returning(iterable)
      .anyNumberOfTimes()
    (iterable.iterator _).when().returns(iterator).anyNumberOfTimes()
    val res = Try(
      collectAllAndCloseStream(
        RefreshingSubspaceStream.fromTypedSubspace(typedSubspace, stubbedDb)))
    assert(res.isFailure)
    assert(res.asInstanceOf[Failure[_]].exception.isInstanceOf[TooManyFailsException])
  }

  it should "fail if one of the keys cannot be decoded" in {
    val allEntities = (1 to 10000).iterator.map(entityFromInt).toList
    addElements(allEntities)
    database.run { tx =>
      val key = typedSubspace.toSubspaceKey(toKey(allEntities(5000)))
      val value = Tuple.from("some value", 1L: lang.Long).pack
      tx.set(key, value)
    }
    val res =
      Try(
        collectAllAndCloseStream(
          RefreshingSubspaceStream.fromTypedSubspace(typedSubspace, database)))
    assert(res.isFailure)
  }

  private def entityFromInt(i: Int): FriendEntity = {
    entity.copy(addedAt = entity.addedAt.plusSeconds(i.toLong), friendId = entity.friendId + i)
  }

  private def toKey(x: FriendEntity): FriendKey =
    FriendKey(ofUserId = x.ofUserId, addedAt = x.addedAt)

  private def entityToKeyValue(x: FriendEntity): KeyValue = {
    val key = typedSubspace.toSubspaceKey(typedSubspace.toKey(x))
    val value = typedSubspace.toRawValue(x)
    new KeyValue(key, value)
  }

  private def collectAll[A](stream: RefreshingSubspaceStream[A]): List[A] = {
    val buffer = ListBuffer.empty[A]
    while (stream.onHasNext().awaitInf) {
      buffer += stream.next()
    }
    buffer.toList
  }

  private def collectAllAndCloseStream[A](stream: RefreshingSubspaceStream[A]): List[A] = {
    val tryResult = Try(collectAll(stream))
    stream.close()
    tryResult.get
  }

  private def asyncIteratorFailedAtTheEnd[A](
      xs: Seq[A],
      exception: Exception = new FDBException("FDB error", 1)): FdbAsyncIterator[A] = {
    asyncIterator(xs, () => Future.failed[lang.Boolean](exception))
  }

  private def asyncIterator[A](
      xs: Seq[A],
      emitOnComplete: () => Future[lang.Boolean]): FdbAsyncIterator[A] = {
    var i = 0
    new FdbAsyncIterator[A] {
      override def onHasNext(): CompletableFuture[lang.Boolean] = {
        if (i < xs.length)
          Future.successful[lang.Boolean](true).toJava.asInstanceOf[CompletableFuture[lang.Boolean]]
        else emitOnComplete().toJava.asInstanceOf[CompletableFuture[lang.Boolean]]
      }
      override def hasNext: Boolean = onHasNext().toScala.await.booleanValue()
      override def next(): A = {
        val x = xs(i)
        i += 1
        x
      }
      override def cancel(): Unit = ()
    }
  }

  private def addElements(xs: List[FriendEntity]): Unit = {
    import DBIO._
    import cats.instances.list._
    import cats.syntax.traverse._
    val dbio = xs.map(typedSubspace.set).sequence[DBIO, Unit]
    dbio.transact(database).await
    ()
  }

  private def slowedDownDatabase: Database = {
    val stubbedDb = stub[Database]
    val stubbedTx = stub[Transaction]
    val stubbedReadTx = mock[ReadTransaction]
    var tx: Transaction = null
    (stubbedDb
      .createTransaction(_: Executor))
      .when(*)
      .onCall { _: Executor =>
        if (tx != null) tx.close()
        tx = database.createTransaction()
        stubbedTx
      }
      .anyNumberOfTimes()
    (stubbedTx.snapshot _).when().returns(stubbedReadTx).anyNumberOfTimes()
    (stubbedTx.close _).when().onCall(_ => tx.close()).anyNumberOfTimes()
    (stubbedReadTx
      .getRange(_: KeySelector, _: KeySelector, _: Int, _: Boolean, _: StreamingMode))
      .expects(*, *, *, *, *)
      .onCall {
        (from: KeySelector, to: KeySelector, limit: Int, reverse: Boolean, mode: StreamingMode) =>
          val realIterable = tx.getRange(from, to, limit, reverse, mode)
          slowedDownIterable(realIterable)
      }
      .anyNumberOfTimes()
    stubbedDb
  }

  private def slowedDownIterable[A](underlying: AsyncIterable[A]): AsyncIterable[A] = {
    new AsyncIterable[A] {
      override def iterator(): FdbAsyncIterator[A] = slowedDownIterator(underlying.iterator())
      override def asList(): CompletableFuture[util.List[A]] = underlying.asList()
    }
  }

  private def slowedDownIterator[A](underlying: FdbAsyncIterator[A]): FdbAsyncIterator[A] = {
    new FdbAsyncIterator[A] {
      override def onHasNext(): CompletableFuture[lang.Boolean] = {
        Future(blocking(Thread.sleep(slowDownEachIterationFor.toMillis)))
          .flatMap(_ => underlying.onHasNext().toScala)
          .toJava
          .asInstanceOf[CompletableFuture[lang.Boolean]]
      }
      override def hasNext: Boolean = underlying.hasNext
      override def next(): A = underlying.next()
      override def cancel(): Unit = underlying.cancel()
    }
  }

}
