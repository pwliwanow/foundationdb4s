package com.github.pwliwanow.foundationdb4s.core
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.tuple.{Tuple, Versionstamp}

import scala.util.Try

/** SubspaceWithVersionstampedValues provides API for interacting with TypedSubspace
  * whose values contain [[Versionstamp]].
  *
  * This trait requires that values are also encoded via [[Tuple]] (just like keys),
  * otherwise it would not be possible to use [[Versionstamp]] correctly.
  */
trait SubspaceWithVersionstampedValues[Entity, Key] extends TypedSubspace[Entity, Key] {

  protected def extractVersionstamp(entity: Entity): Versionstamp
  protected def toTupledValue(entity: Entity): Tuple
  protected def toEntity(key: Key, tupledValue: Tuple): Entity

  def toRawValue(entity: Entity): Array[Byte] = {
    val tupledValue = toTupledValue(entity)
    if (extractVersionstamp(entity).isComplete) tupledValue.pack
    else tupledValue.packWithVersionstamp
  }

  override def toEntity(key: Key, value: Array[Byte]): Entity = {
    toEntity(key, Tuple.fromBytes(value))
  }

  override def set(entity: Entity): DBIO[Unit] = {
    DBIO.fromTransactionToTry { tx =>
      val packedKey = toSubspaceKey(toKey(entity))
      val packedValue = toRawValue(entity)
      val versionstamp = extractVersionstamp(entity)
      if (versionstamp.isComplete) Try(tx.set(packedKey, packedValue))
      else Try(tx.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, packedKey, packedValue))
    }
  }

}
