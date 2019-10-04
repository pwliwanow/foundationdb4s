package com.github.pwliwanow.foundationdb4s.schema

import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.{KeySelector, Range}
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO, TypedSubspace}
import shapeless.{Generic, HList}

import scala.collection.immutable.Seq

object Schema {
  type Aux[E, Key <: HList, Value <: HList] = Schema {
    type Entity = E
    type KeySchema = Key
    type ValueSchema = Value
  }
}

/** Describes structure of keys and values that are to be kept within a subspace.
  *
  * Both KeySchema and ValueSchema provide support for evolutions:
  * it's possible to append `Option[A]` or `List[A]` in newer version of the application,
  * old entries will read this field as `Option.empty` and `List.empty` respectively.
  */
trait Schema { schema =>
  type Entity
  type KeySchema <: HList
  type ValueSchema <: HList

  def toKey(entity: Entity): KeySchema
  def toValue(entity: Entity): ValueSchema
  def toEntity(key: KeySchema, valueRepr: ValueSchema): Entity

  /** [[Namespace]] provides additional type safety by exposing typed API around
    * `range`, `getRange` and `clearRange` operations.
    * It also provides a way to create KeySelectors in a type-safe manner.
    *
    * [[KeySchema]] representation of a key that is to be kept within this subspace
    * [[ValueSchema]] representation of a value that is to be kept within this subspace
    * [[Entity]] the type of the entity that single row (key + value) represents
    */
  class Namespace(override val subspace: Subspace)(
      implicit keyEncoder: ReprEncoder[KeySchema],
      valueEncoder: ReprEncoder[ValueSchema],
      keyDecoder: ReprDecoder[KeySchema],
      valueDecoder: ReprDecoder[ValueSchema])
      extends TypedSubspace[Entity, KeySchema] {

    final override def toKey(entity: Entity): KeySchema = schema.toKey(entity)

    final override def toRawValue(entity: Entity): Array[Byte] =
      valueEncoder.encode(schema.toValue(entity)).pack()

    final override def toTupledKey(key: KeySchema): Tuple =
      keyEncoder.encode(key)

    final override def toKey(tupledKey: Tuple): KeySchema =
      keyDecoder.decode(tupledKey)

    final override def toEntity(key: KeySchema, value: Array[Byte]): Entity = {
      val valueRepr = valueDecoder.decode(Tuple.fromBytes(value))
      schema.toEntity(key, valueRepr)
    }

    def getRow[P <: Product, L <: HList](
        key: P)(implicit gen: Generic.Aux[P, L], keyEv: L =:= KeySchema): ReadDBIO[Option[Entity]] =
      super.get(gen.to(key))

    def clearRow[P <: Product, L <: HList](
        key: P)(implicit gen: Generic.Aux[P, L], keyEv: L =:= KeySchema): DBIO[Unit] =
      super.clear(gen.to(key))

    def clearRange[P <: Product, L <: HList](range: P)(
        implicit gen: Generic.Aux[P, L],
        enc: ReprEncoder[L],
        prefix: Prefix[KeySchema, L]): DBIO[Unit] = {
      this.clearRange(gen.to(range))
    }

    def clearRange[L <: HList](
        range: L)(implicit prefixEv: Prefix[KeySchema, L], enc: ReprEncoder[L]): DBIO[Unit] = {
      super.clear(this.range(range))
    }

    def getRange[P <: Product, L <: HList](range: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): ReadDBIO[Seq[Entity]] = {
      this.getRange(gen.to(range))
    }

    def getRange[L <: HList](range: L)(
        implicit prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): ReadDBIO[Seq[Entity]] = {
      super.getRange(this.range(range))
    }

    def getRange[P <: Product, L <: HList](range: P, limit: Int)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): ReadDBIO[Seq[Entity]] = {
      this.getRange(gen.to(range), limit)
    }

    def getRange[L <: HList](range: L, limit: Int)(
        implicit prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): ReadDBIO[Seq[Entity]] = {
      super.getRange(this.range(range), limit)
    }

    def getRange[P <: Product, L <: HList](range: P, limit: Int, reverse: Boolean)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): ReadDBIO[Seq[Entity]] = {
      this.getRange(gen.to(range), limit, reverse)
    }

    def getRange[L <: HList](range: L, limit: Int, reverse: Boolean)(
        implicit prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): ReadDBIO[Seq[Entity]] = {
      super.getRange(this.range(range), limit, reverse)
    }

    def range[P <: Product, L <: HList](range: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): Range = {
      this.range(gen.to(range))
    }

    def range[L <: HList](
        range: L)(implicit prefixEv: Prefix[KeySchema, L], enc: ReprEncoder[L]): Range = {
      rangeFromHList(range)
    }

    final def firstGreaterOrEqual[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): KeySelector =
      this.firstGreaterOrEqual(gen.to(key))

    final def firstGreaterOrEqual[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: ReprEncoder[L]): KeySelector =
      super.firstGreaterOrEqual(enc.encode(key))

    final def firstGreaterThan[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): KeySelector =
      this.firstGreaterThan(gen.to(key))

    final def firstGreaterThan[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: ReprEncoder[L]): KeySelector =
      super.firstGreaterThan(enc.encode(key))

    final def lastLessOrEqual[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): KeySelector =
      this.lastLessOrEqual(gen.to(key))

    final def lastLessOrEqual[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: ReprEncoder[L]): KeySelector =
      super.lastLessOrEqual(enc.encode(key))

    final def lastLessThan[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: ReprEncoder[L]): KeySelector =
      this.lastLessThan(gen.to(key))

    final def lastLessThan[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: ReprEncoder[L]): KeySelector =
      super.lastLessThan(enc.encode(key))

    private def rangeFromHList[L <: HList](
        range: L)(implicit prefixEv: Prefix[KeySchema, L], enc: ReprEncoder[L]): Range = {
      val (tuple, trailingNulls) = enc.encode(range, new Tuple(), 0)
      // subspace.range adds additional \x00 at the end, so it doesn't select entry that was ending
      // with Option.empty (as those trailing nulls were not inserted),
      // that's why we need to start from subspace.pack here
      val start = subspace.pack(tuple)
      if (trailingNulls == 0) new Range(start, subspace.range(tuple).end)
      else {
        val endingTuple = EncodersHelpers.addNulls(tuple, trailingNulls)
        val end = subspace.range(endingTuple).end
        new Range(start, end)
      }
    }

  }

}
