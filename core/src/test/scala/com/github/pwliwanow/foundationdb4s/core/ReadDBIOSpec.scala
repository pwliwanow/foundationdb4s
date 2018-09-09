package com.github.pwliwanow.foundationdb4s.core

import cats.kernel.laws.IsEq
import cats.laws.MonadLaws
import com.apple.foundationdb.tuple.Tuple

class ReadDBIOSpec extends FoundationDbSpec {

  private val monadLaws = MonadLaws[ReadDBIO]

  it should "satisfy monadLeftIdentity" in {
    val table =
      Table(
        ("element", "element to ReadDBIO"),
        (1, (x: Int) => ReadDBIO.pure(x.toString)),
        (2, (_: Int) => ReadDBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => ReadDBIO[String]) =>
      val isEq = monadLaws.monadLeftIdentity(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy monadRightIdentity" in {
    val table =
      Table("fa", ReadDBIO.pure("Success"), ReadDBIO.failed[String](TestError("Error")))
    forAll(table) { dbio: ReadDBIO[String] =>
      val isEq = monadLaws.monadRightIdentity(dbio)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy kleisliLeftIdentity" in {
    val table =
      Table(
        ("element", "element to ReadDBIO"),
        (1, (x: Int) => ReadDBIO.pure(x.toString)),
        (2, (_: Int) => ReadDBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => ReadDBIO[String]) =>
      val isEq = monadLaws.kleisliLeftIdentity(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy kleisliRightIdentity" in {
    val table =
      Table(
        ("element", "element to ReadDBIO"),
        (1, (x: Int) => ReadDBIO.pure(x.toString)),
        (2, (_: Int) => ReadDBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => ReadDBIO[String]) =>
      val isEq = monadLaws.kleisliRightIdentity(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy mapFlatMapCoherence" in {
    val table =
      Table(
        ("ReadDBIO", "f: A => B"),
        (ReadDBIO.pure(1), (x: Int) => x.toString),
        (ReadDBIO.pure(2), (_: Int) => throw TestError("map error")),
        (ReadDBIO.failed[Int](TestError("failed 3")), (x: Int) => x.toString),
        (ReadDBIO.failed[Int](TestError("failed 4")), (_: Int) => throw TestError("map 4 error"))
      )
    forAll(table) { (x: ReadDBIO[Int], f: Int => String) =>
      val isEq = monadLaws.mapFlatMapCoherence(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy tailRecMStackSafety" in {
    val isEq = monadLaws.tailRecMStackSafety
    assertReadDbioEq(isEq)
  }

  it should "produce DBIO instance after passing DBIO to flatMap" in {
    val lhs = ReadDBIO.pure(10).flatMap(x => DBIO.pure(x.toString))
    val rhs = DBIO.pure("10")
    assertDbioEq(IsEq(lhs, rhs))
  }

  it should "be converted to DBIO correctly" in {
    val table =
      Table(
        ("ReadDBIO", "Expected DBIO"),
        (ReadDBIO.pure(10), DBIO.pure(10)),
        (ReadDBIO.failed[Int](TestError("Failed")), DBIO.failed[Int](TestError("Failed")))
      )
    forAll(table) { (readDbio: ReadDBIO[Int], dbio: DBIO[Int]) =>
      val lhs = ReadDBIO.toDBIO(readDbio)
      val rhs = dbio
      assertDbioEq(IsEq(lhs, rhs))
    }
  }

  it should "be able to read values" in {
    import scala.compat.java8.FutureConverters._
    val key = subspace.pack(Tuple.from("testKey"))
    val value = Tuple.from("value").pack
    testTransactor.db.run(tx => tx.set(key, value))
    val readDbio = ReadDBIO { case (tx, _) => tx.get(key).toScala }
    val _ = await(readDbio.transact(testTransactor))
    val result = {
      val byteArray = await(readDbio.transact(testTransactor))
      Tuple.fromBytes(byteArray).getString(0)
    }
    assert(result === "value")
  }

}
