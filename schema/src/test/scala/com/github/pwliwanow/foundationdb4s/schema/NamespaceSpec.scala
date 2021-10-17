package com.github.pwliwanow.foundationdb4s.schema

import java.nio.ByteBuffer
import java.time.Instant

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.core.FoundationDbSpec
import shapeless.{::, HNil}

object NamespaceSpec {
  case class UserKey(lastName: String, city: Option[String], yearOfBirth: Option[Int])
  case class User(key: UserKey, registeredAt: Instant)
}

object UserSchema extends Schema {
  import NamespaceSpec._
  type Entity = User
  type KeySchema = String :: Option[String] :: Option[Int] :: HNil
  type ValueSchema = Instant :: HNil

  override def toKey(entity: User): KeySchema =
    entity.key.lastName :: entity.key.city :: entity.key.yearOfBirth :: HNil
  override def toValue(entity: User): ValueSchema =
    entity.registeredAt :: HNil
  override def toEntity(key: KeySchema, valueRepr: ValueSchema): User = {
    val registeredAt :: HNil = valueRepr
    val lastName :: city :: yearOfBirth :: HNil = key
    User(UserKey(lastName, city, yearOfBirth), registeredAt)
  }
}

class NamespaceSpec extends FoundationDbSpec { spec =>
  import Prefix._
  import NamespaceSpec._

  object Codecs {
    implicit lazy val instantEncoder: TupleEncoder[Instant] =
      implicitly[TupleEncoder[Long]].contramap(_.toEpochMilli)
    implicit lazy val instantDecoder: TupleDecoder[Instant] =
      implicitly[TupleDecoder[Long]].map(Instant.ofEpochMilli)
  }
  import Codecs._

  val schemaSubspace = new UserSchema.Namespace(spec.subspace)

  private val earlierSubspace = new Subspace(Tuple.from("foundationDbEarlierTestSubspace"))
  private val laterSubspace = new Subspace(Tuple.from("foundationDbTestSubspaceLater"))
  private val allSubspaces = List(earlierSubspace, spec.subspace, laterSubspace)

  private val city = "LA"
  private val lastName = "Smith"
  private val instant = Instant.parse("2018-08-03T10:15:30.00Z")
  private val key = UserKey(lastName, Some(city), Some(1980))
  private val entity = User(key, instant)

  override def afterEach(): Unit = {
    super.afterEach()
    database.run(tx => allSubspaces.map(_.range()).foreach(tx.clear))
  }

  it should "correctly set value in the database" in {
    val (_, v) = entityToTuples(entity)
    schemaSubspace.set(entity).transact(database).await
    val fromDb = get(entity, subspace)
    assert(fromDb === Some(v))
  }

  it should "correctly get value from the database" in {
    val kv = entityToTuples(entity)
    addElements(List(kv), subspace)
    val result =
      schemaSubspace.getRow((lastName, Option(city), Option(1980))).transact(database).await
    assert(result === Some(entity))
  }

  it should "clear elements within provided range" in {
    val toClear = (101 to 200)
      .map(i => key.copy(city = Some(s"$city$i")))
      .map(key => entity.copy(key = key))
    val otherEntities = for {
      otherLastName <- List("A", "Young")
      entity <- toClear
    } yield entity.copy(key = entity.key.copy(lastName = otherLastName))
    val allEntities = toClear ++ otherEntities
    addElements(allEntities.map(entityToTuples), subspace)
    schemaSubspace.clearRange(Tuple1(lastName)).transact(database).await
    val entitiesInDb =
      schemaSubspace.getRange(schemaSubspace.range()).transact(database).await.toSet
    assert(entitiesInDb === otherEntities.toSet)
  }

  it should "return elements within provided range for getRange operation" in {
    val entities = (101 to 200)
      .map(i => key.copy(city = Some(s"$city$i")))
      .map(key => entity.copy(key = key))
    val otherEntities = for {
      otherLastName <- List("A", "Young")
      entity <- entities
    } yield entity.copy(key = entity.key.copy(lastName = otherLastName))
    val allEntities = entities ++ otherEntities
    addElements(allEntities.map(entityToTuples), subspace)
    val dbio = schemaSubspace.getRange(lastName :: HNil)
    val res = dbio.transact(database).await
    val obtainedKeys = res.map(_.key).toSet
    val expectedKeys = entities.map(_.key).toSet
    assert(obtainedKeys === expectedKeys)
  }

  it should "correctly return elements within provided range for getRange operation when range " +
    "ends with None, for Some as last elem" in {
      val entities = (11 to 20)
        .map(i => key.copy(city = None, yearOfBirth = Some(1970 + i)))
        .map(key => entity.copy(key = key))
      val otherEntities = for {
        otherCity <- List("NY")
        entity <- entities
      } yield entity.copy(key = entity.key.copy(city = Some(otherCity)))
      val allEntities = entities ++ otherEntities
      addElements(allEntities.map(entityToTuples), subspace)
      val dbio = schemaSubspace.getRange((lastName, Option.empty[String]))
      val res = dbio.transact(database).await
      val obtainedKeys = res.map(_.key).toSet
      val expectedKeys = entities.map(_.key).toSet
      assert(obtainedKeys === expectedKeys)
    }

  it should "correctly return elements within provided range for getRange operation when range " +
    "ends with None, for None as last elem" in {
      val otherEntity1 = User(UserKey("", None, None), instant)
      val targetEntity = User(UserKey(lastName, None, None), instant)
      val otherEntity2 = User(UserKey(lastName, Option(city), None), instant)
      val otherEntity3 = User(UserKey("Young", None, None), instant)
      val allEntities = List(otherEntity1, targetEntity, otherEntity2, otherEntity3)
      addElements(allEntities.map(entityToTuples), subspace)
      val dbio = schemaSubspace.getRange((lastName, Option.empty[String]))
      val res = dbio.transact(database).await
      val obtainedKeys = res.map(_.key).toSet
      val expectedKeys = Set(targetEntity.key)
      assert(obtainedKeys === expectedKeys)
    }

  it should "return limited number of elements within provided range for getRange operation" in {
    val entities = (101 to 200)
      .map(i => key.copy(city = Some(s"$city$i")))
      .map(key => entity.copy(key = key))
    val otherEntities = for {
      otherLastName <- List("A", "Young")
      entity <- entities
    } yield entity.copy(key = entity.key.copy(lastName = otherLastName))
    val allEntities = entities ++ otherEntities
    addElements(allEntities.map(entityToTuples), subspace)
    val dbio = schemaSubspace.getRange(Tuple1(lastName), 10)
    val res = dbio.transact(database).await
    val obtainedKeys = res.map(_.key).toSet
    val expectedKeys = entities.take(10).map(_.key).toSet
    assert(obtainedKeys === expectedKeys)
  }

  it should "correctly return elements within provided range, for given limit and reverse flag " +
    "for getRange operation when range ends with None, for Some as last elem" in {
      val entities = (11 to 20)
        .map(i => key.copy(city = None, yearOfBirth = Some(1970 + i)))
        .map(key => entity.copy(key = key))
      val otherEntities = for {
        otherCity <- List("NY")
        entity <- entities
      } yield entity.copy(key = entity.key.copy(city = Some(otherCity)))
      val allEntities = entities ++ otherEntities
      addElements(allEntities.map(entityToTuples), subspace)
      val dbio = schemaSubspace.getRange((lastName, Option.empty[String]), 5, reverse = true)
      val res = dbio.transact(database).await
      val obtainedKeys = res.map(_.key).toSet
      val expectedKeys = entities.takeRight(5).map(_.key).toSet
      assert(obtainedKeys === expectedKeys)
    }

  it should "correctly create firstGreaterOrEqual KeySelector" in {
    val from = schemaSubspace.firstGreaterOrEqual((lastName, Option.empty[String]))
    val expected = KeySelector.firstGreaterOrEqual(subspace.pack(Tuple.from(lastName)))
    assert(from === expected)
  }

  it should "correctly create firstGreaterThan KeySelector" in {
    val from = schemaSubspace.firstGreaterThan((lastName, Option(city)))
    val expected = KeySelector.firstGreaterThan(subspace.pack(Tuple.from(lastName, city)))
    assert(from === expected)
  }

  it should "correctly create lastLessOrEqual KeySelector" in {
    val from = schemaSubspace.lastLessOrEqual((lastName, Option(city)))
    val expected = KeySelector.lastLessOrEqual(subspace.pack(Tuple.from(lastName, city)))
    assert(from === expected)
  }

  it should "correctly create lastLessThan KeySelector" in {
    val from = schemaSubspace.lastLessThan(Tuple1("name"))
    val expected = KeySelector.lastLessThan(subspace.pack(Tuple.from("name")))
    assert(from === expected)
  }

  it should "correctly select elements based on KeySelectors" in {
    val entities = (11 to 20)
      .map(i => key.copy(city = None, yearOfBirth = Some(1970 + i)))
      .map(key => entity.copy(key = key))
    val otherEntities = for {
      otherCity <- List("NY")
      entity <- entities
    } yield entity.copy(key = entity.key.copy(city = Some(otherCity)))
    val allEntities = entities ++ otherEntities
    addElements(allEntities.map(entityToTuples), subspace)
    val from = schemaSubspace.firstGreaterOrEqual((lastName, Option.empty[String]))
    val to =
      schemaSubspace.firstGreaterOrEqual((lastName, Option.empty[String], Option(1985)))
    val res = schemaSubspace.getRange(from, to).transact(database).await
    val obtainedKeys = res.map(_.key).toSet
    val expectedKeys = entities.take(4).map(_.key).toSet
    assert(obtainedKeys === expectedKeys)
  }

  private def entityToTuples(entity: User): (Tuple, Tuple) = {
    val key = {
      val acc = new Tuple().add(entity.key.lastName)
      (entity.key.city, entity.key.yearOfBirth) match {
        case (Some(c), Some(yearOfBirth)) =>
          acc.add(c).addInt(yearOfBirth)
        case (Some(c), None) =>
          acc.add(c)
        case (None, Some(yearOfBirth)) =>
          acc.addObject(null).addInt(yearOfBirth)
        case (None, None) =>
          acc
      }
    }
    val value = new Tuple().add(entity.registeredAt.toEpochMilli)
    key -> value
  }

  private def get(entity: User, from: Subspace): Option[Tuple] = {
    val (k, _) = entityToTuples(entity)
    get(k, from)
  }

  private def get(key: Tuple, from: Subspace): Option[Tuple] = {
    Option(database.runAsync(tx => tx.get(from.pack(key))).get).map(Tuple.fromBytes)
  }

  private implicit class TupleHolder(val tuple: Tuple) {
    def addInt(value: Int): Tuple = {
      val output = new Array[Byte](4)
      ByteBuffer.wrap(output).putInt(value)
      tuple.add(output)
    }
  }

  private def addElements(kvs: Seq[(Tuple, Tuple)], to: Subspace): Unit = {
    database.run { tx =>
      kvs.foreach { case (k, v) =>
        tx.set(to.pack(k), v.pack)
      }
    }
  }
}
