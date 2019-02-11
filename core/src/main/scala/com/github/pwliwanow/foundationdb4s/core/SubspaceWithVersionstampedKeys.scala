package com.github.pwliwanow.foundationdb4s.core
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.tuple.Versionstamp

import scala.util.Try

/** SubspaceWithVersionstampedKeys provides API for interacting with TypedSubspace
  * whose Keys contain [[Versionstamp]].
  */
trait SubspaceWithVersionstampedKeys[Entity, Key] extends TypedSubspace[Entity, Key] {

  protected def extractVersionstamp(key: Key): Versionstamp

  override def toSubspaceKey(key: Key): Array[Byte] = {
    val tupledKey = toTupledKey(key)
    val versionstamp = extractVersionstamp(key)
    if (versionstamp.isComplete) subspace.pack(tupledKey)
    else subspace.packWithVersionstamp(tupledKey)
  }

  /** Saves provided entity in the database.
    * Note that if the Key contains incomplete [[Versionstamp]],
    * it will not be possible to use `get` method to fetch the entity in the same transaction.
    */
  override def set(entity: Entity): DBIO[Unit] = {
    DBIO.fromTransactionToTry { tx =>
      val packedValue = toRawValue(entity)
      val key = toKey(entity)
      val versionstamp = extractVersionstamp(key)
      val packedKey = toSubspaceKey(key)
      if (versionstamp.isComplete) Try(tx.set(packedKey, packedValue))
      else Try(tx.mutate(MutationType.SET_VERSIONSTAMPED_KEY, packedKey, packedValue))
    }
  }

}
