package com.github.pwliwanow.foundationdb4s.schema

import java.nio.ByteBuffer
import java.time.Instant

import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.schema.TestCodecs.Age
import org.scalatest.flatspec.AnyFlatSpec

object TestCodecs {
  case class Age(value: Int) extends AnyVal

  case class Inner(age: Age)
  case class TestKey(name: String, inner: Inner)

  case class Test(
      key: TestKey,
      value: Long,
      maybeAnotherTestKey: Option[TestKey],
      maybeInt: Option[Int],
      maybeLong: Option[Long],
      maybeString: Option[String],
      listOfStrings: List[String],
      listOfTestKeys: List[TestKey])

  implicit lazy val innerDec: TupleDecoder[Inner] = TupleDecoder.derive[Inner]
  implicit lazy val innerEnc: TupleEncoder[Inner] = TupleEncoder.derive[Inner]

  implicit lazy val testKeyEnc: TupleEncoder[TestKey] = TupleEncoder.derive[TestKey]
  implicit lazy val testKeyDec: TupleDecoder[TestKey] = TupleDecoder.derive[TestKey]

  lazy val testEnc: TupleEncoder[Test] = TupleEncoder.derive[Test]
  implicit lazy val testDec: TupleDecoder[Test] = TupleDecoder.derive[Test]
}

object OverridenCodecs {
  implicit lazy val customIntDec: TupleDecoder[Int] =
    implicitly[TupleDecoder[Long]].map(_.toInt)
  implicit lazy val customIntEnc: TupleEncoder[Int] =
    implicitly[TupleEncoder[Long]].contramap[Int](_.toLong)
}

class EncoderDecoderSpec extends AnyFlatSpec {
  it should "encode and decode back values with missing values" in {
    import TestCodecs._
    val value =
      Test(
        key = TestKey("name", Inner(Age(30))),
        value = 1111102L,
        maybeAnotherTestKey = None,
        maybeInt = None,
        maybeLong = None,
        maybeString = None,
        listOfStrings = List.empty,
        listOfTestKeys = List.empty
      )
    val encoded = testEnc.encode(value)
    val decoded = testDec.decode(encoded)
    assert(decoded === value)
  }

  it should "encode and decode back values with present values" in {
    import TestCodecs._
    val value =
      Test(
        key = TestKey("name", Inner(Age(30))),
        value = 1111102L,
        maybeAnotherTestKey = Some(TestKey("sth", Inner(Age(51)))),
        maybeInt = Some(94),
        maybeLong = Some(39243543L),
        maybeString = Some("testString"),
        listOfStrings = List("1", "2"),
        listOfTestKeys = List(
          TestKey("s", Inner(Age(41))),
          TestKey("s2", Inner(Age(42))),
          TestKey("s3", Inner(Age(43))))
      )
    val encoded = testEnc.encode(value)
    val decoded = testDec.decode(encoded)
    assert(decoded === value)
  }

  it should "encode and decode back values with few missing and few present fields" in {
    import TestCodecs._
    val value =
      Test(
        key = TestKey("name", Inner(Age(30))),
        value = 1111102L,
        maybeAnotherTestKey = None,
        maybeInt = Some(94),
        maybeLong = None,
        maybeString = Some("testString"),
        listOfStrings = List.empty,
        listOfTestKeys = List(
          TestKey("s", Inner(Age(41))),
          TestKey("s2", Inner(Age(42))),
          TestKey("s3", Inner(Age(43))))
      )
    val encoded = testEnc.encode(value)
    val decoded = testDec.decode(encoded)
    assert(decoded === value)
  }

  it should "correctly encode values" in {
    import TestCodecs._
    val value =
      Test(
        key = TestKey("name", Inner(Age(30))),
        value = 1111102L,
        maybeAnotherTestKey = None,
        maybeInt = Some(94),
        maybeLong = None,
        maybeString = Some("testString"),
        listOfStrings = List("test"),
        listOfTestKeys = List.empty
      )
    val encoded = testEnc.encode(value)
    val expected =
      new Tuple()
        .add(new Tuple().add("name").add(new Tuple().add(encodeInt(30))))
        .add(1111102L)
        .addObject(null)
        .add(encodeInt(94))
        .addObject(null)
        .add("testString")
        .add(new Tuple().add("test"))
    assert(encoded === expected)
  }

  it should "correctly encode int with acc" in {
    testEncoderWithAcc(10, result => decodeInt(result.getBytes(0)))
  }

  it should "correctly encode anyval with int as underlying value" in {
    testEncoderWithAcc[Age](Age(30), result => Age(decodeInt(result.getBytes(0))))
  }

  it should "correctly encode int without acc" in {
    testEncoderWithoutAcc(10, result => decodeInt(result.getBytes(0)))
  }

  it should "correctly encode long with acc" in {
    testEncoderWithAcc(10L, _.getLong(0))
  }

  it should "correctly encode long without acc" in {
    testEncoderWithoutAcc(10L, _.getLong(0))
  }

  it should "correctly encode string with acc" in {
    testEncoderWithAcc("test", _.getString(0))
  }

  it should "correctly encode string without acc" in {
    testEncoderWithoutAcc("test", _.getString(0))
  }

  it should "correctly encode boolean with acc" in {
    testEncoderWithAcc(true, _.getBoolean(0))
  }

  it should "correctly encode boolean without acc" in {
    testEncoderWithoutAcc(false, _.getBoolean(0))
  }

  it should "correctly encode present option with acc" in {
    testEncoderWithAcc(Some("test"), tuple => Option(tuple.getString(0)))
  }

  it should "correctly encode present option without acc" in {
    testEncoderWithoutAcc(Some("test"), tuple => Option(tuple.getString(0)))
  }

  it should "correctly encode empty option with acc" in {
    val holder = new Tuple()
    val (result, proceedingNulls) = implicitly[TupleEncoder[Option[String]]].encode(None, holder, 0)
    assert(proceedingNulls === 1)
    assert(result.isEmpty)
  }

  it should "correctly encode empty option without acc" in {
    val result = implicitly[TupleEncoder[Option[String]]].encode(None)
    assert(result === new Tuple().addObject(null))
  }

  it should "correctly encode custom encoder obtained from contramap without acc" in {
    implicit val instantEnc: TupleEncoder[Instant] =
      implicitly[TupleEncoder[Long]].contramap(_.toEpochMilli)
    val instant = Instant.parse("2018-12-03T10:15:30.00Z")
    testEncoderWithoutAcc(instant, tuple => Instant.ofEpochMilli(tuple.getLong(0)))
  }

  it should "use custom encoder over predefined encoder" in {
    import OverridenCodecs._
    val value = 4102
    val encoder = implicitly[TupleEncoder[Int]]
    val encoded = encoder.encode(value)
    val decoded = encoded.getLong(0).toInt
    assert(decoded === value)
  }

  it should "correctly decode int" in {
    testDecoder(new Tuple().add(encodeInt(22)), 22)
  }

  it should "correctly decode anyval with int as underlying value" in {
    testDecoder[Age](new Tuple().add(encodeInt(22)), Age(22))
  }

  it should "correctly decode long" in {
    testDecoder(new Tuple().add(1231231231L), 1231231231L)
  }

  it should "correctly decode boolean" in {
    testDecoder(new Tuple().add(true), true)
  }

  it should "correctly decode string" in {
    testDecoder(new Tuple().add("Some value"), "Some value")
  }

  it should "correctly decode option" in {
    testDecoder(new Tuple().add("Other value"), Option("Other value"))
  }

  it should "correctly decode using custom decoder obtained from map" in {
    implicit val instantDec: TupleDecoder[Instant] =
      implicitly[TupleDecoder[Long]].map(Instant.ofEpochMilli)
    val instant = Instant.parse("2018-12-03T10:15:30.00Z")
    testDecoder(new Tuple().add(instant.toEpochMilli), instant)
  }

  it should "use custom decoder over predefined decoder" in {
    import OverridenCodecs._
    val value = 4102
    val tuple = new Tuple().add(value.toLong)
    val decoder = implicitly[TupleDecoder[Int]]
    val decoded = decoder.decode(tuple)
    assert(decoded === value)
  }

  private def testDecoder[A: TupleDecoder](input: Tuple, expected: A): Unit =
    assert(implicitly[TupleDecoder[A]].decode(input) === expected)

  private def testEncoderWithAcc[A: TupleEncoder](inputValue: A, decode: Tuple => A): Unit = {
    val acc = new Tuple()
    val (result, _) = implicitly[TupleEncoder[A]].encode(inputValue, acc, 0)
    assert(decode(result) === inputValue)
  }

  private def testEncoderWithoutAcc[A: TupleEncoder](inputValue: A, decode: Tuple => A): Unit = {
    val result = implicitly[TupleEncoder[A]].encode(inputValue)
    assert(decode(result) === inputValue)
  }

  private def encodeInt(value: Int): Array[Byte] = {
    val output = new Array[Byte](4)
    ByteBuffer.wrap(output).putInt(value)
    output
  }

  private def decodeInt(bytes: Array[Byte]): Int = ByteBuffer.wrap(bytes).getInt
}
