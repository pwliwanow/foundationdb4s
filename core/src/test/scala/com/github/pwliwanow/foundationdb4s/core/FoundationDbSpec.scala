package com.github.pwliwanow.foundationdb4s.core

import java.time.Instant

import cats.laws.IsEq
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{Database, FDB, KeySelector}
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Assertion, BeforeAndAfterEach}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

trait FoundationDbSpec
    extends AnyFlatSpecLike
    with TableDrivenPropertyChecks
    with BeforeAndAfterEach {
  spec =>

  implicit def ec: ExecutionContextExecutor = DatabaseHolder.ec
  val subspace = new Subspace(Tuple.from("foundationDbTestSubspace"))

  lazy val database = DatabaseHolder.database

  protected val typedSubspace = new TypedSubspace[FriendEntity, FriendKey] {
    override val subspace: Subspace = spec.subspace

    override def toKey(entity: FriendEntity): FriendKey =
      FriendKey(ofUserId = entity.ofUserId, addedAt = entity.addedAt)

    override def toRawValue(entity: FriendEntity): Array[Byte] =
      new Tuple().add(entity.friendId).add(entity.friendName).pack

    override protected def toTupledKey(key: FriendKey): Tuple = {
      new Tuple().add(key.ofUserId).add(key.addedAt.toEpochMilli)
    }

    override protected def toKey(tupledKey: Tuple): FriendKey = {
      val addedAt = Instant.ofEpochMilli(tupledKey.getLong(1))
      FriendKey(ofUserId = tupledKey.getLong(0), addedAt = addedAt)
    }

    override protected def toEntity(key: FriendKey, value: Array[Byte]): FriendEntity = {
      val tupledValue = Tuple.fromBytes(value)
      FriendEntity(
        ofUserId = key.ofUserId,
        addedAt = key.addedAt,
        friendId = tupledValue.getLong(0),
        friendName = tupledValue.getString(1))
    }
  }

  override def afterEach(): Unit = {
    database.run(_.clear(subspace.range()))
  }

  def assertDbioEq[A](isEqDbio: IsEq[DBIO[A]]): Assertion = {
    assertEq[A, DBIO](isEqDbio, _.transact(database))
  }

  def assertReadDbioEq[A](isEqReadDbio: IsEq[ReadDBIO[A]]): Assertion = {
    assertEq[A, ReadDBIO](isEqReadDbio, _.transact(database))
  }

  implicit class FutureHolder[A](future: Future[A]) {
    def await: A = await(3.seconds)
    def await(duration: Duration): A = Await.result(future, duration)
    def awaitInf: A = await(Duration.Inf)
  }

  protected implicit val keySelectorEquality: Equality[KeySelector] =
    new Equality[KeySelector] {
      override def areEqual(a: KeySelector, b: Any): Boolean = {
        if (b == null || !b.isInstanceOf[KeySelector]) return false
        val bSelector = b.asInstanceOf[KeySelector]
        a.toString == bSelector.toString
      }
    }

  // as this functions uses Thread.sleep, use it only in core.
  // When there is Akka (or something similar) in scope,
  // use suitable methods provided by their testkit
  protected[core] def awaitResult(assertion: => Boolean)(
      checkInterval: Duration = 50.millis,
      maxWaitTime: Duration = 3.seconds): Unit = {
    @tailrec
    def loop(maxEndTime: Long): Unit = {
      Thread.sleep(checkInterval.toMillis)
      Try(assert(assertion)) match {
        case Success(_) => ()
        case Failure(ex) =>
          if (maxEndTime < System.nanoTime()) throw ex
          else loop(maxEndTime)
      }
    }
    val maxEndTime = System.nanoTime() + maxWaitTime.toNanos
    loop(maxEndTime)
  }

  private def assertEq[A, F[_]](isEqDbio: IsEq[F[A]], f: F[A] => Future[A]): Assertion = {
    val leftFuture = f(isEqDbio.lhs)
    val rightFuture = f(isEqDbio.rhs)
    val (left, right) = (Try(leftFuture.await), Try(rightFuture.await))
    assertTry(left, right)
  }

  private def assertTry[A](left: Try[A], right: Try[A]): Assertion =
    (left, right) match {
      case (Failure(l), Failure(r)) =>
        assert(l.getMessage === r.getMessage)
      case _ =>
        assert(left === right)
    }
}

final case class FriendEntity(ofUserId: Long, addedAt: Instant, friendId: Long, friendName: String)

final case class FriendKey(ofUserId: Long, addedAt: Instant)

final case class TestError(msg: String) extends RuntimeException

object DatabaseHolder {
  implicit def ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  val database: Database = FDB.selectAPIVersion(620).open(null, ec)
  sys.addShutdownHook(database.close())
}
