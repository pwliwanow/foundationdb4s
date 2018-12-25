package com.github.pwliwanow.foundationdb4s.core

import cats.laws.MonadLaws
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}

import scala.concurrent.Future
import scala.compat.java8.FutureConverters._
import scala.util.{Success, Try}

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

  it should "properly compose multiple operations" in {
    val key = subspace.pack(Tuple.from("testKey"))
    val value = Tuple.from("value").pack
    val dbioValue = for {
      _ <- DBIO { case (tx, _)     => Future.fromTry(Try(tx.set(key, value))) }
      bytes <- DBIO { case (tx, _) => tx.get(key).toScala }
    } yield Tuple.fromBytes(bytes).getString(0)
    val valueFromTx = await(dbioValue.transact(testTransactor))
    assert(valueFromTx === "value")
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

  forAll(Table("userVersion", 0, 10)) { userVersion: Int =>
    it should s"get correct versionstamp for userVersion = $userVersion" in {
      val packedTuple =
        subspace.packWithVersionstamp(Tuple.from("testKey", Versionstamp.incomplete(userVersion)))
      val modifyDbio = DBIO { (tx, _) =>
        Future.fromTry {
          Try(tx.mutate(MutationType.SET_VERSIONSTAMPED_KEY, packedTuple, Array.emptyByteArray))
        }
      }
      val (_, Some(versionstamp)) =
        await(modifyDbio.transactVersionstamped(testTransactor, userVersion))
      val expected = testTransactor.db.run { tx =>
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
    val result = Try(await(dbio.transactVersionstamped(testTransactor))).map { case (v, _) => v }
    assert(result === Success(value))
  }

}
