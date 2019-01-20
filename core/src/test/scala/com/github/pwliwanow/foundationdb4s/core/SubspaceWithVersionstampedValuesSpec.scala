package com.github.pwliwanow.foundationdb4s.core
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}

class SubspaceWithVersionstampedValuesSpec extends FoundationDbSpec { spec =>

  private case class Event(key: String, value: Versionstamp)

  private val eventSubspace = new SubspaceWithVersionstampedValues[Event, String] {
    override val subspace: Subspace = spec.subspace
    override def extractVersionstamp(entity: Event): Versionstamp = entity.value
    override def toKey(entity: Event): String = entity.key
    override def toTupledValue(entity: Event): Tuple = Tuple.from(entity.value)
    override def toTupledKey(key: String): Tuple = Tuple.from(key)
    override def toKey(tupledKey: Tuple): String = tupledKey.getString(0)
    override def toEntity(key: String, tupledValue: Tuple): Event =
      Event(key = key, value = tupledValue.getVersionstamp(0))
  }

  val userVersions = Table("userVersion", 0, 13)

  forAll(userVersions) { userVersion: Int =>
    it should s"set entity correctly when keys contain incomplete versionstamp [$userVersion]" in {
      val key = "someKey"
      val event = Event(key = key, Versionstamp.incomplete(userVersion))
      val setDbio = eventSubspace.set(event)
      val (_, Some(versionstamp)) =
        setDbio.transactVersionstamped(testTransactor, userVersion).await
      val result = eventSubspace.get(key).transact(testTransactor).await
      assert(result === Some(Event(key, versionstamp)))
    }
  }

  forAll(userVersions) { userVersion: Int =>
    it should s"set entity correctly when keys contain complete versionstamp [$userVersion]" in {
      val key = "otherKey"
      val versionstamp = Versionstamp.complete(Array.fill[Byte](10)(0x0: Byte), userVersion)
      val event = Event(key, versionstamp)
      eventSubspace.set(event).transact(testTransactor).await
      val result = eventSubspace.get(key).transact(testTransactor).await
      assert(result === Some(Event(key, versionstamp)))
    }
  }

}
