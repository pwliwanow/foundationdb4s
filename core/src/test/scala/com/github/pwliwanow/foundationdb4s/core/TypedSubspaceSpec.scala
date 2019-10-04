package com.github.pwliwanow.foundationdb4s.core
import java.time.Instant

import com.apple.foundationdb.{KeySelector, KeyValue}
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple

import scala.concurrent.Future
import scala.concurrent.duration._

class TypedSubspaceSpec extends FoundationDbSpec { spec =>

  private val earlierSubspace = new Subspace(Tuple.from("foundationDbEarlierTestSubspace"))
  private val laterSubspace = new Subspace(Tuple.from("foundationDbTestSubspaceLater"))
  private val allSubspaces = List(earlierSubspace, spec.subspace, laterSubspace)

  private val entity =
    FriendEntity(
      ofUserId = 1L,
      addedAt = Instant.parse("2018-08-03T10:15:30.00Z"),
      friendId = 10L,
      friendName = "John")
  private val entityKey = FriendKey(ofUserId = entity.ofUserId, addedAt = entity.addedAt)
  private val (key, value) = entityToTuples(entity)

  override def afterEach(): Unit = {
    super.afterEach()
    database.run(tx => allSubspaces.map(_.range()).foreach(tx.clear))
  }

  it should "properly calculate subspace key" in {
    val calculated = typedSubspace.toSubspaceKey(entityKey)
    val expected = subspace.pack(key)
    assert(calculated === expected)
  }

  it should "properly transform KeyValue into Entity" in {
    val packedKey = subspace.pack(key)
    val calculated = typedSubspace.toEntity(new KeyValue(packedKey, value.pack()))
    assert(calculated === entity)
  }

  it should "clear the entire subspace" in {
    allSubspaces.foreach(s => addElement(key, value, s))
    val dbio = typedSubspace.clear()
    dbio.transact(database).await
    assert(get(key, earlierSubspace).isDefined)
    assert(get(key, laterSubspace).isDefined)
    assert(get(key, subspace).isEmpty)
  }

  it should "clear key" in {
    allSubspaces.foreach(s => addElement(key, value, s))
    val dbio = typedSubspace.clear(entityKey)
    dbio.transact(database).await
    assert(get(key, earlierSubspace).isDefined)
    assert(get(key, laterSubspace).isDefined)
    assert(get(key, subspace).isEmpty)
  }

  it should "not clear anything if provided key does not exist in db" in {
    allSubspaces.foreach(s => addElement(key, value, s))
    val dbio = typedSubspace.clear(entityKey.copy(ofUserId = 2L))
    dbio.transact(database).await
    assert(get(key, earlierSubspace).isDefined)
    assert(get(key, laterSubspace).isDefined)
    assert(get(key, subspace).isDefined)
  }

  it should "clear all elements in range" in {
    val entities = List(
      entity,
      entity.copy(ofUserId = 2L),
      entity.copy(ofUserId = 2L, addedAt = Instant.parse("2018-06-03T10:15:30.00Z")),
      entity.copy(addedAt = Instant.parse("2018-07-13T11:15:30.00Z")),
      entity.copy(ofUserId = 2L, addedAt = Instant.parse("2018-10-13T11:15:30.00Z")),
      entity.copy(ofUserId = 1L)
    )
    addElements(entities.map(entityToTuples), subspace)
    val dbio = typedSubspace.clear(typedSubspace.range(new Tuple().add(2L)))
    dbio.transact(database).await
    assert(get(entity, subspace).isDefined)
    assert(get(entities(1), subspace).isEmpty)
    assert(get(entities(2), subspace).isEmpty)
    assert(get(entities(3), subspace).isDefined)
    assert(get(entities(4), subspace).isEmpty)
    assert(get(entities(5), subspace).isDefined)
  }

  it should "clear all elements between specified keys" in {
    val entities = List(
      entity,
      entity.copy(ofUserId = 2L),
      entity.copy(addedAt = Instant.parse("2018-06-03T10:15:30.00Z")),
      entity.copy(addedAt = Instant.parse("2018-07-13T11:15:30.00Z")),
      entity.copy(addedAt = Instant.parse("2018-10-13T11:15:30.00Z")),
      entity.copy(ofUserId = 1L)
    )
    addElements(entities.map(entityToTuples), subspace)
    val from = FriendKey(ofUserId = 1L, addedAt = Instant.parse("2018-06-03T10:15:30.00Z"))
    val to = FriendKey(ofUserId = 1L, addedAt = Instant.parse("2018-08-03T10:15:30.00Z"))
    val dbio = typedSubspace.clear(from, to)
    dbio.transact(database).await
    assert(get(entity, subspace).isDefined)
    assert(get(entities(1), subspace).isDefined)
    assert(get(entities(2), subspace).isEmpty)
    assert(get(entities(3), subspace).isEmpty)
    assert(get(entities(4), subspace).isDefined)
    assert(get(entities(5), subspace).isDefined)
  }

  it should "return option empty for get operation if provided key does no exist" in {
    val dbio = typedSubspace.get(entityKey)
    val res = dbio.transact(database).await
    assert(res === Option.empty[FriendEntity])
  }

  it should "return entity for get operation if entity for provided key exists" in {
    addElement(key, value, subspace)
    val dbio = typedSubspace.get(entityKey)
    val res = dbio.transact(database).await
    assert(res === Some(entity))
  }

  it should "return limited number of elements for getRange operation" in {
    val entities = (101L to 200L)
      .map(i => entity.copy(addedAt = entity.addedAt.plusSeconds(i), friendId = i))
    addElements(entities.map(entityToTuples), subspace)
    val from = FriendKey(0L, Instant.parse("2007-12-03T10:15:30.00Z"))
    val to = FriendKey(Long.MaxValue, Instant.parse("2027-12-03T10:15:30.00Z"))
    val dbio = typedSubspace.getRange(from, to)
    val res = dbio.transact(database).await
    assert(res.size === 50)
    val obtainedFriendIds = res.map(_.friendId).toList
    val expectedFriendIds = (101L to 150L).toList
    assert(obtainedFriendIds === expectedFriendIds)
  }

  it should "return elements within provided range for getRange operation" in {
    val entities = (101L to 200L)
      .map(i => entity.copy(addedAt = entity.addedAt.plusSeconds(i), friendId = i))
    addElements(entities.map(entityToTuples), subspace)
    val range = subspace.range(new Tuple().add(1L))
    val dbio = typedSubspace.getRange(range)
    val res = dbio.transact(database).await
    assert(res.size === 100)
    val obtainedFriendIds = res.map(_.friendId).toList
    val expectedFriendIds = (101L to 200L).toList
    assert(obtainedFriendIds === expectedFriendIds)
  }

  it should "return elements within provided range and specified limit for getRange operation" in {
    val entities = (101L to 200L)
      .map(i => entity.copy(addedAt = entity.addedAt.plusSeconds(i), friendId = i))
    addElements(entities.map(entityToTuples), subspace)
    val range = subspace.range(new Tuple().add(1L))
    val dbio = typedSubspace.getRange(range, 30)
    val res = dbio.transact(database).await
    assert(res.size === 30)
    val obtainedFriendIds = res.map(_.friendId).toList
    val expectedFriendIds = (101L to 130L).toList
    assert(obtainedFriendIds === expectedFriendIds)
  }

  it should "return elements within provided range,specified limit and reverse flag for getRange operation" in {
    val reverse = true
    val entities = (101L to 200L)
      .map(i => entity.copy(addedAt = entity.addedAt.plusSeconds(i), friendId = i))
    addElements(entities.map(entityToTuples), subspace)
    val range = subspace.range(new Tuple().add(1L))
    val dbio = typedSubspace.getRange(range, 40, reverse)
    val res = dbio.transact(database).await
    assert(res.size === 40)
    val obtainedFriendIds = res.map(_.friendId).toList
    val expectedFriendIds = (161L to 200L).reverse.toList
    assert(obtainedFriendIds === expectedFriendIds)
  }

  it should "return elements within specified keySelectors for getRange operation" in {
    val entities = (101L to 200L)
      .map(i => entity.copy(addedAt = entity.addedAt.plusSeconds(i), friendId = i))
    addElements(entities.map(entityToTuples), subspace)
    val (beginKey, _) = entityToTuples(entities(59))
    val (endKey, _) = entityToTuples(entities(79))
    val from = KeySelector.firstGreaterOrEqual(subspace.pack(beginKey))
    val to = KeySelector.firstGreaterOrEqual(subspace.pack(endKey))
    val dbio = typedSubspace.getRange(from, to)
    val res = dbio.transact(database).await
    assert(res.size === 20)
    val obtainedFriendIds = res.map(_.friendId).toList
    val expectedFriendIds = (160L to 179L).toList
    assert(obtainedFriendIds === expectedFriendIds)
  }

  it should "return elements within specified keySelectors and with specified limit for getRange operation" in {
    val entities = (101L to 200L)
      .map(i => entity.copy(addedAt = entity.addedAt.plusSeconds(i), friendId = i))
    addElements(entities.map(entityToTuples), subspace)
    val (beginKey, _) = entityToTuples(entities(59))
    val (endKey, _) = entityToTuples(entities(79))
    val from = KeySelector.firstGreaterOrEqual(subspace.pack(beginKey))
    val to = KeySelector.firstGreaterOrEqual(subspace.pack(endKey))
    val dbio = typedSubspace.getRange(from, to, 10)
    val res = dbio.transact(database).await
    assert(res.size === 10)
    val obtainedFriendIds = res.map(_.friendId).toList
    val expectedFriendIds = (160L to 169L).toList
    assert(obtainedFriendIds === expectedFriendIds)
  }

  it should "return elements within specified keySelectors and with specified limit and reverse flag for getRange operation" in {
    val entities = (101L to 200L)
      .map(i => entity.copy(addedAt = entity.addedAt.plusSeconds(i), friendId = i))
    addElements(entities.map(entityToTuples), subspace)
    val (beginKey, _) = entityToTuples(entities(59))
    val (endKey, _) = entityToTuples(entities(79))
    val from = KeySelector.lastLessOrEqual(subspace.pack(beginKey))
    val to = KeySelector.firstGreaterOrEqual(subspace.pack(endKey)).add(1)
    val dbio = typedSubspace.getRange(from, to, 10, reverse = true)
    val res = dbio.transact(database).await
    assert(res.size === 10)
    val obtainedFriendIds = res.map(_.friendId).toList
    val expectedFriendIds = (171L to 180L).reverse.toList
    assert(obtainedFriendIds === expectedFriendIds)
  }

  it should "insert value if it doesn't exist" in {
    val dbio = typedSubspace.set(entity)
    dbio.transact(database).await
    assert(get(key, subspace) === Some(value))
  }

  it should "update value if key already exists" in {
    addElement(key, value, subspace)
    val entity = spec.entity.copy(friendName = "New friend")
    val dbio = typedSubspace.set(entity)
    dbio.transact(database).await
    assert(get(key, subspace) === Some(new Tuple().add(entity.friendId).add("New friend")))
  }

  it should "create correct range" in {
    assert(typedSubspace.range() === subspace.range())
  }

  it should "create correct range for given tuple" in {
    val tuple = Tuple.from("01").add(1L)
    assert(typedSubspace.range(tuple) === subspace.range(tuple))
  }

  it should "create firstGreaterOrEqual KeySelector correctly" in {
    val obtained = typedSubspace.firstGreaterOrEqual(entityKey)
    val expected = KeySelector.firstGreaterOrEqual(subspace.pack(key))
    assert(obtained === expected)
  }

  it should "create firstGreaterOrEqual KeySelector correctly for given tuple" in {
    val tuple = Tuple.from("01", "Test")
    val obtained = typedSubspace.firstGreaterOrEqual(tuple)
    val expected = KeySelector.firstGreaterOrEqual(subspace.pack(tuple))
    assert(obtained === expected)
  }

  it should "create firstGreaterThan KeySelector correctly" in {
    val obtained = typedSubspace.firstGreaterThan(entityKey)
    val expected = KeySelector.firstGreaterThan(subspace.pack(key))
    assert(obtained === expected)
  }

  it should "create firstGreaterThan KeySelector correctly for given tuple" in {
    val tuple = Tuple.from("01", "Test")
    val obtained = typedSubspace.firstGreaterThan(tuple)
    val expected = KeySelector.firstGreaterThan(subspace.pack(tuple))
    assert(obtained === expected)
  }

  it should "create lastLessOrEqual keySelector correctly" in {
    val obtained = typedSubspace.lastLessOrEqual(entityKey)
    val expected = KeySelector.lastLessOrEqual(subspace.pack(key))
    assert(obtained === expected)
  }

  it should "create lastLessOrEqual keySelector correctly for given tuple" in {
    val tuple = Tuple.from("01", "Test")
    val obtained = typedSubspace.lastLessOrEqual(tuple)
    val expected = KeySelector.lastLessOrEqual(subspace.pack(tuple))
    assert(obtained === expected)
  }

  it should "create lastLessThan keySelector correctly" in {
    val obtained = typedSubspace.lastLessThan(entityKey)
    val expected = KeySelector.lastLessThan(subspace.pack(key))
    assert(obtained === expected)
  }

  it should "create lastLessThan keySelector correctly for given tuple" in {
    val tuple = Tuple.from("01", "Test")
    val obtained = typedSubspace.lastLessThan(tuple)
    val expected = KeySelector.lastLessThan(subspace.pack(tuple))
    assert(obtained === expected)
  }

  it should "be able to watch a key" in {
    addElement(key, value, subspace)
    val dbio = typedSubspace.watch(entityKey)
    val res = dbio.transact(database).await
    assert(!res.isCompleted)
    Future {
      val changedEntity = entity.copy(friendName = "New name")
      val (_, newValue) = entityToTuples(changedEntity)
      addElement(key, newValue, subspace)
    }.await
    awaitResult(res.isCompleted)(maxWaitTime = 1.second)
  }

  private def addElement(key: Tuple, value: Tuple, to: Subspace): Unit = {
    addElements(List(key -> value), to)
  }

  private def addElements(kvs: Seq[(Tuple, Tuple)], to: Subspace): Unit = {
    database.run { tx =>
      kvs.foreach {
        case (k, v) =>
          tx.set(to.pack(k), v.pack)
      }
    }
  }

  private def get(entity: FriendEntity, from: Subspace): Option[Tuple] = {
    val (k, _) = entityToTuples(entity)
    get(k, from)
  }

  private def get(key: Tuple, from: Subspace): Option[Tuple] = {
    Option(database.runAsync(tx => tx.get(from.pack(key))).get).map(Tuple.fromBytes)
  }

  private def entityToTuples(entity: FriendEntity): (Tuple, Tuple) = {
    val k = new Tuple().add(entity.ofUserId).add(entity.addedAt.toEpochMilli)
    val v = new Tuple().add(entity.friendId).add(entity.friendName)
    k -> v
  }

}
