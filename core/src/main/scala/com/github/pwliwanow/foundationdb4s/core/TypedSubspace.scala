package com.github.pwliwanow.foundationdb4s.core

import com.apple.foundationdb.{KeySelector, KeyValue, Range}
import com.apple.foundationdb.async.AsyncIterable
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple

import scala.collection.convert.ImplicitConversions._
import scala.compat.java8.FutureConverters._
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

/** TypedSubspace is a wrapper around FoundationDB API exposing typed (where possible)
  * operations within provided Subspace.
  *
  * It assumes that row keys are to be serialized via [[Tuple]].
  *
  * @tparam Entity the type of the entity that single row (key + value) represents
  * @tparam Key the type of the entity that single key row represents
  */
trait TypedSubspace[Entity, Key] {
  val subspace: Subspace

  def toKey(entity: Entity): Key
  def toRawValue(entity: Entity): Array[Byte]

  /** Resulting Tuple will be packed by the subspace before executing all operations. */
  protected def toTupledKey(key: Key): Tuple
  protected def toKey(tupledKey: Tuple): Key
  protected def toEntity(key: Key, value: Array[Byte]): Entity

  final def toSubspaceKey(key: Key): Array[Byte] = {
    subspace.pack(toTupledKey(key))
  }

  final def toEntity(keyValue: KeyValue): Entity = {
    val keyRepr = toKey(subspace.unpack(keyValue.getKey))
    toEntity(keyRepr, keyValue.getValue)
  }

  final def clear(): DBIO[Unit] = DBIO {
    case (tx, _) =>
      Future.fromTry(Try(tx.clear(subspace.range())))
  }

  final def clear(key: Key): DBIO[Unit] = DBIO {
    case (tx, _) =>
      val packedKey = toSubspaceKey(key)
      Future.fromTry(Try(tx.clear(packedKey)))
  }

  final def clear(from: Key, to: Key): DBIO[Unit] = DBIO {
    case (tx, _) =>
      val packedFromKey = toSubspaceKey(from)
      val packedToKey = toSubspaceKey(to)
      Future.fromTry(Try(tx.clear(packedFromKey, packedToKey)))
  }

  final def get(key: Key): ReadDBIO[Option[Entity]] = ReadDBIO {
    case (tx, ec) =>
      val packedKey = toSubspaceKey(key)
      tx.get(packedKey)
        .toScala
        .map { byteArray =>
          Option(byteArray).map(toEntity(key, _))
        }(ec)
  }

  def getRange(from: Key, to: Key): ReadDBIO[Seq[Entity]] = getRange(from, to, 50)

  final def getRange(from: Key, to: Key, limit: Int): ReadDBIO[Seq[Entity]] = {
    getRange(from, to, limit, reverse = false)
  }

  final def getRange(from: Key, to: Key, limit: Int, reverse: Boolean): ReadDBIO[Seq[Entity]] =
    ReadDBIO {
      case (tx, ec) =>
        val packedFromKey = toSubspaceKey(from)
        val packedToKey = toSubspaceKey(to)
        val keyValues = tx.getRange(packedFromKey, packedToKey, limit, reverse)
        toRangeResult(keyValues)(ec)
    }

  final def getRange(range: Range): ReadDBIO[Seq[Entity]] = ReadDBIO {
    case (tx, ec) =>
      toRangeResult(tx.getRange(range))(ec)
  }

  final def getRange(range: Range, limit: Int): ReadDBIO[Seq[Entity]] =
    getRange(range, limit, reverse = false)

  final def getRange(range: Range, limit: Int, reverse: Boolean): ReadDBIO[Seq[Entity]] = ReadDBIO {
    case (tx, ec) =>
      toRangeResult(tx.getRange(range, limit, reverse))(ec)
  }

  final def getRange(from: KeySelector, to: KeySelector): ReadDBIO[Seq[Entity]] = ReadDBIO {
    case (tx, ec) =>
      toRangeResult(tx.getRange(from, to))(ec)
  }

  final def getRange(from: KeySelector, to: KeySelector, limit: Int): ReadDBIO[Seq[Entity]] =
    ReadDBIO {
      case (tx, ec) =>
        toRangeResult(tx.getRange(from, to, limit))(ec)
    }

  final def getRange(
      from: KeySelector,
      to: KeySelector,
      limit: Int,
      reverse: Boolean): ReadDBIO[Seq[Entity]] = ReadDBIO {
    case (tx, ec) =>
      toRangeResult(tx.getRange(from, to, limit, reverse))(ec)
  }

  final def set(entity: Entity): DBIO[Unit] = DBIO {
    case (tx, _) =>
      val packedKey = toSubspaceKey(toKey(entity))
      val packedValue = toRawValue(entity)
      Future.fromTry(Try(tx.set(packedKey, packedValue)))
  }

  final def range(): Range = subspace.range()

  final def range(tuple: Tuple): Range = subspace.range(tuple)

  final def firstGreaterOrEqual(key: Key): KeySelector =
    KeySelector.firstGreaterOrEqual(toSubspaceKey(key))

  final def firstGreaterOrEqual(key: Tuple): KeySelector =
    KeySelector.firstGreaterOrEqual(subspace.pack(key.pack()))

  final def firstGreaterThan(key: Key): KeySelector =
    KeySelector.firstGreaterThan(toSubspaceKey(key))

  final def firstGreaterThan(key: Tuple): KeySelector =
    KeySelector.firstGreaterThan(subspace.pack(key.pack()))

  final def lastLessOrEqual(key: Key): KeySelector =
    KeySelector.lastLessOrEqual(toSubspaceKey(key))

  final def lastLessOrEqual(key: Tuple): KeySelector =
    KeySelector.lastLessOrEqual(subspace.pack(key.pack()))

  final def lastLessThan(key: Key): KeySelector =
    KeySelector.lastLessThan(toSubspaceKey(key))

  final def lastLessThan(key: Tuple): KeySelector =
    KeySelector.lastLessThan(subspace.pack(key.pack()))

  private def toRangeResult(keyValues: AsyncIterable[KeyValue])(
      implicit ec: ExecutionContextExecutor): Future[Seq[Entity]] = {
    keyValues
      .asList()
      .toScala
      .map { javaList =>
        javaList
          .iterator()
          .map(toEntity)
          .toList
      }(ec)
  }
}
