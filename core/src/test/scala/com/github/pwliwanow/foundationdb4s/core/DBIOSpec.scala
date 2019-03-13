package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function

import cats.laws.MonadLaws
import com.apple.foundationdb._
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}
import com.github.pwliwanow.foundationdb4s.core.Pet.{Cat, Dog}
import org.scalamock.scalatest.MockFactory

import scala.util.{Failure, Success, Try}

class DBIOSpec extends FoundationDbSpec with MockFactory {

  private val monadLaws = MonadLaws[DBIO]

  it should "satisfy monadLeftIdentity" in {
    val table =
      Table(
        ("element", "element to DBIO"),
        (1, (x: Int) => DBIO.pure(x.toString)),
        (2, (_: Int) => DBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => DBIO[String]) =>
      val isEq = monadLaws.monadLeftIdentity(x, f)
      assertDbioEq(isEq)
    }
  }

  it should "satisfy monadRightIdentity" in {
    val table =
      Table("fa", DBIO.pure("Success"), DBIO.failed[String](TestError("Error")))
    forAll(table) { dbio: DBIO[String] =>
      val isEq = monadLaws.monadRightIdentity(dbio)
      assertDbioEq(isEq)
    }
  }

  it should "satisfy kleisliLeftIdentity" in {
    val table =
      Table(
        ("element", "element to DBIO"),
        (1, (x: Int) => DBIO.pure(x.toString)),
        (2, (_: Int) => DBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => DBIO[String]) =>
      val isEq = monadLaws.kleisliLeftIdentity(x, f)
      assertDbioEq(isEq)
    }
  }

  it should "satisfy kleisliRightIdentity" in {
    val table =
      Table(
        ("element", "element to DBIO"),
        (1, (x: Int) => DBIO.pure(x.toString)),
        (2, (_: Int) => DBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => DBIO[String]) =>
      val isEq = monadLaws.kleisliRightIdentity(x, f)
      assertDbioEq(isEq)
    }
  }

  it should "satisfy mapFlatMapCoherence" in {
    val table =
      Table(
        ("DBIO", "f: A => B"),
        (DBIO.pure(1), (x: Int) => x.toString),
        (DBIO.pure(2), (_: Int) => throw TestError("map error")),
        (DBIO.failed[Int](TestError("failed 3")), (x: Int) => x.toString),
        (DBIO.failed[Int](TestError("failed 4")), (_: Int) => throw TestError("map 4 error"))
      )

    forAll(table) { (x: DBIO[Int], f: Int => String) =>
      val isEq = monadLaws.mapFlatMapCoherence(x, f)
      assertDbioEq(isEq)
    }
  }

  it should "satisfy tailRecMStackSafety" in {
    val isEq = monadLaws.tailRecMStackSafety
    assertDbioEq(isEq)
  }

  it should "not fail with stack overflow for deeply nested combination of TryAction and FlatMaps" in {
    val action = (_: Transaction) => Try("some value")
    val n = 50000
    val deeplyNested = (1 to n).foldLeft(DBIO.pure("")) { (dbio, _) =>
      dbio.flatMap(_ => DBIO.fromTransactionToTry(action))
    }
    deeplyNested.transact(contextWithNullTransaction)
  }

  it should "not fail with stack overflow for deeply nested combination of FutureAction and FlatMaps" in {
    val action = (_: Transaction) => CompletableFuture.supplyAsync[String](() => "some value")
    val n = 50000
    val deeplyNested = (1 to n).foldLeft(DBIO.pure("")) { (dbio, _) =>
      dbio.flatMap(_ => DBIO.fromTransactionToPromise(action))
    }
    deeplyNested.transact(contextWithNullTransaction)
  }

  it should "commit transaction if dbio is successful" in {
    val key = subspace.pack(Tuple.from("testKey"))
    val value = Tuple.from("value").pack
    val dbio = for {
      _ <- DBIO.fromTransactionToTry(tx => Try(tx.set(key, value)))
      _ <- DBIO.pure[Unit](())
    } yield ()
    dbio.transact(database).await
    val valueFromDb: Array[Byte] = database.runAsync(tx => tx.get(key)).get()
    assert(Tuple.fromBytes(valueFromDb).getString(0) === "value")
  }

  it should "properly compose multiple operations" in {
    val key = subspace.pack(Tuple.from("testKey"))
    val value = Tuple.from("value").pack
    val dbioValue = for {
      _ <- DBIO.fromTransactionToTry(tx => Try(tx.set(key, value)))
      bytes <- DBIO.fromTransactionToPromise(tx => tx.get(key))
    } yield Tuple.fromBytes(bytes).getString(0)
    val valueFromTx = dbioValue.transact(database).await
    assert(valueFromTx === "value")
  }

  it should "rollback transaction if dbio is a failure and failed with exception that caused failure" in {
    val key = subspace.pack(Tuple.from("testKey"))
    val error = TestError("Failure")
    val failedDbio = for {
      _ <- DBIO.fromTransactionToTry(tx => Try(tx.set(key, Tuple.from("value").pack)))
      _ <- DBIO.failed[Unit](TestError("Failure"))
    } yield ()
    val tryResult = Try(failedDbio.transact(database).await)
    val value: Array[Byte] = database.runAsync(tx => tx.get(key)).get()

    assert(tryResult === Failure(error))
    assert(value === null)
  }

  forAll(Table("userVersion", 0, 10)) { userVersion: Int =>
    it should s"get correct versionstamp for userVersion = $userVersion" in {
      val packedTuple =
        subspace.packWithVersionstamp(Tuple.from("testKey", Versionstamp.incomplete(userVersion)))
      val modifyDbio = DBIO.fromTransactionToTry { tx =>
        Try(tx.mutate(MutationType.SET_VERSIONSTAMPED_KEY, packedTuple, Array.emptyByteArray))
      }
      val (_, Some(versionstamp)) =
        modifyDbio.transactVersionstamped(database, userVersion).await
      val expected = database.run { tx =>
        val serialized = tx.getRange(subspace.range(Tuple.from("testKey")), 1).iterator.next.getKey
        val tuple = subspace.unpack(serialized)
        tuple.getVersionstamp(1)
      }
      assert(versionstamp === expected)
    }
  }

  it should "not fail when transactVersiontamped is called and DBIO does not modify database" in {
    val value = "This dbio does not modify any data"
    val dbio = DBIO.pure(value)
    val result = Try(dbio.transactVersionstamped(database).await).map { case (v, _) => v }
    assert(result === Success(value))
  }

  it should "correctly handle covariance" in {
    import cats.implicits._
    val cat = Cat("Tom")
    val dog = Dog("Rico")
    val expected = List(cat, dog)
    val catDbio: DBIO[Pet] =
      DBIO.fromTransactionToPromise(_ => CompletableFuture.supplyAsync[Pet](() => cat))
    val dogDbio: DBIO[Dog] =
      DBIO.fromTransactionToPromise(_ => CompletableFuture.supplyAsync(() => dog))
    val dbioPets: DBIO[List[Pet]] = List(catDbio, dogDbio).sequence
    val received = dbioPets.transact(database).await
    assert(received === expected)
  }

  private def contextWithNullTransaction: TransactionContext = new TransactionContext {
    override def run[T](retryable: function.Function[_ >: Transaction, T]): T = {
      retryable(null)
    }

    override def runAsync[T](
        retryable: function.Function[_ >: Transaction, _ <: CompletableFuture[T]])
      : CompletableFuture[T] = {
      retryable(null)
    }

    override def read[T](retryable: function.Function[_ >: ReadTransaction, T]): T = {
      retryable(null)
    }

    override def readAsync[T](
        retryable: function.Function[_ >: ReadTransaction, _ <: CompletableFuture[T]])
      : CompletableFuture[T] = {
      retryable(null)
    }

    override def getExecutor: Executor = ec
  }

}
