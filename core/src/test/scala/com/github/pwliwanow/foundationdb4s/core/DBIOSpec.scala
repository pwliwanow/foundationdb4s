package com.github.pwliwanow.foundationdb4s.core
import cats.laws.MonadLaws
import com.apple.foundationdb.tuple.Tuple

import scala.concurrent.Future
import scala.util.Try

class DBIOSpec extends FoundationDbSpec {

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

  it should "commit transaction if dbio is successful" in {
    val key = subspace.pack(Tuple.from("testKey"))
    val value = Tuple.from("value").pack
    val dbio = for {
      _ <- DBIO {
        case (tx, _) =>
          Future.fromTry(Try(tx.set(key, value)))
      }
      _ <- DBIO.pure[Unit](())
    } yield ()
    await(dbio.transact(testTransactor))
    val valueFromDb: Array[Byte] = testTransactor.db.runAsync(tx => tx.get(key)).get()

    assert(Tuple.fromBytes(valueFromDb).getString(0) === "value")
  }

  it should "rollback transaction if dbio is a failure" in {
    val key = subspace.pack(Tuple.from("testKey"))
    val failedDbio = for {
      _ <- DBIO {
        case (tx, _) =>
          Future.fromTry(Try(tx.set(key, Tuple.from("value").pack)))
      }
      _ <- DBIO.failed[Unit](TestError("Failure"))
    } yield ()
    val tryResult = Try(await(failedDbio.transact(testTransactor)))
    val value: Array[Byte] = testTransactor.db.runAsync(tx => tx.get(key)).get()

    assert(tryResult.isFailure)
    assert(value === null)
  }

}
