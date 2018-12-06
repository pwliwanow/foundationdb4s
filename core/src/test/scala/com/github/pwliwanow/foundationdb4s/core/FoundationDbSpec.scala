package com.github.pwliwanow.foundationdb4s.core

import java.time.Instant

import cats.laws.IsEq
import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import org.scalactic.Equality
import org.scalatest.{Assertion, BeforeAndAfterEach, FlatSpecLike}
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Try}

trait FoundationDbSpec extends FlatSpecLike with TableDrivenPropertyChecks with BeforeAndAfterEach {
  spec =>

  implicit def ec: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  val subspace = new Subspace(Tuple.from("foundationDbTestSubspace"))

  lazy val testTransactor = Transactor(version = 600)

  protected val typedSubspace = new TypedSubspace[FriendEntity, FriendKey] {
    override val subspace: Subspace = spec.subspace

    override def toKey(entity: FriendEntity): FriendKey =
      FriendKey(ofUserId = entity.ofUserId, addedAt = entity.addedAt)

    override def toRawValue(entity: FriendEntity): Array[Byte] =
      Tuple.from(entity.friendId, entity.friendName).pack

    override protected def toTupledKey(key: FriendKey): Tuple =
      Tuple.from(key.ofUserId).add(key.addedAt.toEpochMilli)

    override protected def toKey(tupledKey: Tuple): FriendKey = {
      val addedAt = Instant.ofEpochMilli(tupledKey.getLong(1))
      FriendKey(ofUserId = tupledKey.getString(0), addedAt = addedAt)
    }

    override protected def toEntity(key: FriendKey, value: Array[Byte]): FriendEntity = {
      val tupledValue = Tuple.fromBytes(value)
      FriendEntity(
        ofUserId = key.ofUserId,
        addedAt = key.addedAt,
        friendId = tupledValue.getString(0),
        friendName = tupledValue.getString(1))
    }
  }

  override def afterEach(): Unit = {
    testTransactor.db.run(_.clear(subspace.range()))
  }

  def assertDbioEq[A](isEqDbio: IsEq[DBIO[A]]): Assertion = {
    assertEq[A, DBIO](isEqDbio, _.transact(testTransactor))
  }

  def assertReadDbioEq[A](isEqReadDbio: IsEq[ReadDBIO[A]]): Assertion = {
    assertEq[A, ReadDBIO](isEqReadDbio, _.transact(testTransactor))
  }

  protected def await[A](future: Future[A]): A = Await.result(future, 3.second)

  protected def awaitInf[A](future: Future[A]): A = Await.result(future, Duration.Inf)

  protected implicit val keySelectorEquality: Equality[KeySelector] =
    new Equality[KeySelector] {
      override def areEqual(a: KeySelector, b: Any): Boolean = {
        if (b == null || !b.isInstanceOf[KeySelector]) return false
        val bSelector = b.asInstanceOf[KeySelector]
        a.toString == bSelector.toString
      }
    }

  private def assertEq[A, F[_]](isEqDbio: IsEq[F[A]], f: F[A] => Future[A]): Assertion = {
    val leftFuture = f(isEqDbio.lhs)
    val rightFuture = f(isEqDbio.rhs)
    val (left, right) = (Try(await(leftFuture)), Try(await(rightFuture)))
    assertTry(left, right)
  }

  private def assertTry[A](left: Try[A], right: Try[A]): Assertion = (left, right) match {
    case (Failure(l), Failure(r)) =>
      assert(l.getMessage === r.getMessage)
    case _ =>
      assert(left === right)
  }

}

final case class FriendEntity(
    ofUserId: String,
    addedAt: Instant,
    friendId: String,
    friendName: String)

final case class FriendKey(ofUserId: String, addedAt: Instant)

final case class TestError(msg: String) extends RuntimeException
