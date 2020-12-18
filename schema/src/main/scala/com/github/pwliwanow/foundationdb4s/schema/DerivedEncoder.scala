package com.github.pwliwanow.foundationdb4s.schema

import com.apple.foundationdb.tuple.Tuple
import shapeless.{::, Generic, HList, HNil, Lazy}

abstract class DerivedEncoder[A] extends TupleEncoder[A]

object DerivedEncoder {
  implicit def deriveEncoder[A, R](implicit
      gen: Generic.Aux[A, R],
      enc: Lazy[ReprEncoder[R]]): DerivedEncoder[A] =
    new DerivedEncoder[A] {
      override def encode(value: A): Tuple = {
        val (res, _) = enc.value.encode(gen.to(value), new Tuple, 0)
        res
      }
      override def encode(value: A, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
        (EncodersHelpers.addNulls(acc, preceedingNulls).add(encode(value)), 0)
    }
}

/** Represents TupleEncoder for HList-based structures. */
abstract class ReprEncoder[A] extends TupleEncoder[A]

/** Provides automatic derivation for HList-based structures. */
object ReprEncoder {
  implicit object HnilEncoder extends ReprEncoder[HNil] {
    override def encode(value: HNil): Tuple = new Tuple()
    override def encode(value: HNil, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (acc, preceedingNulls)
  }

  implicit def hlistEncoder[H, T <: HList](implicit
      hEncoder: TupleEncoder[H],
      tEncoder: Lazy[ReprEncoder[T]]): ReprEncoder[H :: T] =
    new ReprEncoder[H :: T] {
      override def encode(value: H :: T): Tuple = {
        val (res, _) = encode(value, new Tuple(), 0)
        res
      }
      override def encode(value: H :: T, acc: Tuple, preceedingNulls: Int): (Tuple, Int) = {
        val h :: t = value
        val (hEncoded, numberOfNulls) = hEncoder.encode(h, acc, preceedingNulls)
        tEncoder.value.encode(t, hEncoded, numberOfNulls)
      }
    }
}
