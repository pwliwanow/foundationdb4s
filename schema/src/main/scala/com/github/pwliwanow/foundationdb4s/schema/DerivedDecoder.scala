package com.github.pwliwanow.foundationdb4s.schema

import com.apple.foundationdb.tuple.Tuple
import shapeless.{::, Generic, HList, HNil, Lazy}

abstract class DerivedDecoder[A] extends TupleDecoder[A]

object DerivedDecoder {
  implicit def deriveDecoder[A, R](
      implicit gen: Generic.Aux[A, R],
      dec: Lazy[ReprDecoder[R]]): DerivedDecoder[A] =
    new DerivedDecoder[A] {
      override def decode(tuple: Tuple): A = gen.from(dec.value.decode(tuple, 0))
      override def decode(tuple: Tuple, index: Int): A = {
        // whole "class" is wrapped in a tuple
        val nested = tuple.getNestedTuple(index)
        decode(nested)
      }
    }
}

/** Represents TupleDecoder for HList-based structures. */
abstract class ReprDecoder[A] extends TupleDecoder[A]

/** Provides automatic derivation for HList-based structures. */
object ReprDecoder {
  implicit object HnilDecoder extends ReprDecoder[HNil] {
    override def decode(tuple: Tuple): HNil = HNil
    override def decode(tuple: Tuple, index: Int): HNil = HNil
  }

  implicit def hlistDecoder[H, T <: HList](
      implicit hDecoder: TupleDecoder[H],
      tDecoder: Lazy[ReprDecoder[T]]): ReprDecoder[H :: T] = new ReprDecoder[H :: T] {
    override def decode(tuple: Tuple): H :: T = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): H :: T = {
      val h = hDecoder.decode(tuple, index)
      val t = tDecoder.value.decode(tuple, index + 1)
      h :: t
    }
  }
}
