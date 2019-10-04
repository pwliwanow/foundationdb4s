package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.CompletableFuture

import com.apple.foundationdb.async.AsyncIterable
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.{KeySelector, KeyValue, Range}
import com.github.pwliwanow.foundationdb4s.core.internal.CompletableFutureHolder._

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.Promise
import scala.util.{Success, Try}

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

  def toSubspaceKey(key: Key): Array[Byte] = {
    subspace.pack(toTupledKey(key))
  }

  final def toEntity(keyValue: KeyValue): Entity = {
    val keyRepr = toKey(subspace.unpack(keyValue.getKey))
    toEntity(keyRepr, keyValue.getValue)
  }

  final def clear(): DBIO[Unit] = {
    DBIO.fromTransactionToTry(tx => Try(tx.clear(subspace.range())))
  }

  final def clear(key: Key): DBIO[Unit] = {
    val packedKey = toSubspaceKey(key)
    DBIO.fromTransactionToTry(tx => Try(tx.clear(packedKey)))
  }

  final def clear(range: Range): DBIO[Unit] = {
    DBIO.fromTransactionToTry(tx => Try(tx.clear(range)))
  }

  final def clear(from: Key, to: Key): DBIO[Unit] = {
    val packedFromKey = toSubspaceKey(from)
    val packedToKey = toSubspaceKey(to)
    DBIO.fromTransactionToTry(tx => Try(tx.clear(packedFromKey, packedToKey)))
  }

  final def get(key: Key): ReadDBIO[Option[Entity]] = {
    val packedKey = toSubspaceKey(key)
    ReadDBIO.fromTransactionToPromise { tx =>
      tx.get(packedKey).thenApply[Option[Entity]] { byteArray =>
        Option(byteArray).map(toEntity(key, _))
      }
    }
  }

  def getRange(from: Key, to: Key): ReadDBIO[Seq[Entity]] = getRange(from, to, 50)

  final def getRange(from: Key, to: Key, limit: Int): ReadDBIO[Seq[Entity]] = {
    getRange(from, to, limit, reverse = false)
  }

  final def getRange(from: Key, to: Key, limit: Int, reverse: Boolean): ReadDBIO[Seq[Entity]] = {
    val packedFromKey = toSubspaceKey(from)
    val packedToKey = toSubspaceKey(to)
    ReadDBIO.fromTransactionToPromise { tx =>
      val keyValues = tx.getRange(packedFromKey, packedToKey, limit, reverse)
      toRangeResult(keyValues)
    }
  }

  final def getRange(range: Range): ReadDBIO[Seq[Entity]] = {
    ReadDBIO.fromTransactionToPromise(tx => toRangeResult(tx.getRange(range)))
  }

  final def getRange(range: Range, limit: Int): ReadDBIO[Seq[Entity]] =
    getRange(range, limit, reverse = false)

  final def getRange(range: Range, limit: Int, reverse: Boolean): ReadDBIO[Seq[Entity]] = {
    ReadDBIO.fromTransactionToPromise(tx => toRangeResult(tx.getRange(range, limit, reverse)))
  }

  final def getRange(from: KeySelector, to: KeySelector): ReadDBIO[Seq[Entity]] = {
    ReadDBIO.fromTransactionToPromise(tx => toRangeResult(tx.getRange(from, to)))
  }

  final def getRange(from: KeySelector, to: KeySelector, limit: Int): ReadDBIO[Seq[Entity]] = {
    ReadDBIO.fromTransactionToPromise(tx => toRangeResult(tx.getRange(from, to, limit)))
  }

  final def getRange(
      from: KeySelector,
      to: KeySelector,
      limit: Int,
      reverse: Boolean): ReadDBIO[Seq[Entity]] = {
    ReadDBIO.fromTransactionToPromise(tx => toRangeResult(tx.getRange(from, to, limit, reverse)))
  }

  def set(entity: Entity): DBIO[Unit] = {
    DBIO.fromTransactionToTry { tx =>
      val packedKey = toSubspaceKey(toKey(entity))
      val packedValue = toRawValue(entity)
      Try(tx.set(packedKey, packedValue))
    }
  }

  final def range(): Range = subspace.range()

  final def range(tuple: Tuple): Range = subspace.range(tuple)

  final def firstGreaterOrEqual(key: Key): KeySelector =
    KeySelector.firstGreaterOrEqual(toSubspaceKey(key))

  final def firstGreaterOrEqual(key: Tuple): KeySelector =
    KeySelector.firstGreaterOrEqual(subspace.pack(key))

  final def firstGreaterThan(key: Key): KeySelector =
    KeySelector.firstGreaterThan(toSubspaceKey(key))

  final def firstGreaterThan(key: Tuple): KeySelector =
    KeySelector.firstGreaterThan(subspace.pack(key))

  final def lastLessOrEqual(key: Key): KeySelector =
    KeySelector.lastLessOrEqual(toSubspaceKey(key))

  final def lastLessOrEqual(key: Tuple): KeySelector =
    KeySelector.lastLessOrEqual(subspace.pack(key))

  final def lastLessThan(key: Key): KeySelector =
    KeySelector.lastLessThan(toSubspaceKey(key))

  final def lastLessThan(key: Tuple): KeySelector =
    KeySelector.lastLessThan(subspace.pack(key))

  final def watch(key: Key): DBIO[Promise[Unit]] = {
    watchJava(key).map(_.toPromise)
  }

  final def watchJava(key: Key): DBIO[CompletableFuture[Unit]] = {
    DBIO.fromTransactionToTry { tx =>
      Success(tx.watch(toSubspaceKey(key)).thenApply[Unit](_ => ()))
    }
  }

  private def toRangeResult(keyValues: AsyncIterable[KeyValue]): CompletableFuture[Seq[Entity]] = {
    keyValues
      .asList()
      .thenApply[Seq[Entity]] { javaList =>
        javaList
          .iterator()
          .asScala
          .map(toEntity)
          .toList
      }
  }
}
