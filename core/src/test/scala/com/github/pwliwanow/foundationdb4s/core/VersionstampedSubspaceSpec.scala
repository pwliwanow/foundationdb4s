package com.github.pwliwanow.foundationdb4s.core

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}

class VersionstampedSubspaceSpec extends FoundationDbSpec { spec =>

  private case class Event(key: Versionstamp, value: String)

  private val eventSubspace = new VersionstampedSubspace[Event, Versionstamp] {
    override val subspace: Subspace = spec.subspace
    override def extractVersionstamp(key: Versionstamp): Versionstamp = key
    override def toKey(entity: Event): Versionstamp = entity.key
    override def toRawValue(entity: Event): Array[Byte] = Tuple.from(entity.value).pack
    override def toTupledKey(key: Versionstamp): Tuple = Tuple.from(key)
    override def toKey(tupledKey: Tuple): Versionstamp = tupledKey.getVersionstamp(0)
    override def toEntity(key: Versionstamp, value: Array[Byte]): Event = {
      Event(key = key, value = Tuple.fromBytes(value).getString(0))
    }
  }

  val userVersions = Table("userVersion", 0, 18)

  forAll(userVersions) { userVersion: Int =>
    it should s"set entity correctly when keys contain incomplete versionstamp [$userVersion]" in {
      val value = """{ "content": "some value" }"""
      val event = Event(Versionstamp.incomplete(userVersion), value)
      val setDbio = eventSubspace.set(event)
      val (_, Some(versionstamp)) =
        await(setDbio.transactVersionstamped(testTransactor, userVersion))
      val getDbio = eventSubspace.get(versionstamp).transact(testTransactor)
      assert(await(getDbio) === Some(Event(versionstamp, value)))
    }
  }

  forAll(userVersions) { userVersion: Int =>
    it should s"set entity correctly when keys contain complete versionstamp [$userVersion]" in {
      val value = """{ "content": "some other value" }"""
      val versionstamp = Versionstamp.complete(Array.fill[Byte](10)(0x0: Byte), userVersion)
      val event = Event(versionstamp, value)
      val setDbio = eventSubspace.set(event)
      await(setDbio.transact(testTransactor))
      val getDbio = eventSubspace.get(versionstamp).transact(testTransactor)
      assert(await(getDbio) === Some(Event(versionstamp, value)))
    }
  }

}
