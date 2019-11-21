package com.github.pwliwanow.foundationdb4s.schema
import java.nio.ByteBuffer

import com.apple.foundationdb.tuple.Tuple
import shapeless.Unwrapped

/** A type class that provides conversion from type [[A]] to [[Tuple]]
  * (that is used internally by FoundationDB).
  *
  * All implementations must respect backwards compatibility.
  */
trait TupleEncoder[A] { self =>

  /** Encodes value as a [[Tuple]]. */
  def encode(value: A): Tuple

  /** Appends encoded value to provided [[Tuple]] (`acc` parameter).
    *
    * If `preceedingNulls` is positive and provided value does not represent
    * empty value, then nulls should be appended before the encoded value.
    * Otherwise value should not be appended and number of preceeding nulls
    * should be returned in the result.
    */
  def encode(value: A, acc: Tuple, preceedingNulls: Int): (Tuple, Int)

  /** Create new instance of [[TupleEncoder]] by applying function `f` to a value
    * before encoding it as an [[A]].
    *
    * If possible it should be used instead of implementing custom `TupleEncoder[B]`
    * from scratch, as this method is unlikely to change.
    */
  def contramap[B](f: B => A): TupleEncoder[B] = new TupleEncoder[B] {
    override def encode(value: B): Tuple = self.encode(f(value))
    override def encode(value: B, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      self.encode(f(value), acc, preceedingNulls)
  }
}

object TupleEncoder extends BasicEncoders {
  /** Derives [[TupleEncoder]] for [[A]] type parameter.
    *
    * Derivation will succeed only if there exist implicit
    * encoders for all members of [[A]].
    */
  def derive[A](implicit enc: DerivedEncoder[A]): TupleEncoder[A] = enc
}

trait BasicEncoders {
  import EncodersHelpers._

  implicit object IntEncoder extends TupleEncoder[Int] {
    override def encode(value: Int): Tuple = new Tuple().add(encodeInt(value))
    override def encode(value: Int, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(encodeInt(value)), 0)
  }

  implicit object LongEncoder extends TupleEncoder[Long] {
    override def encode(value: Long): Tuple = new Tuple().add(value)
    override def encode(value: Long, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(value), 0)
  }

  implicit object StringEncoder extends TupleEncoder[String] {
    override def encode(value: String): Tuple = new Tuple().add(value)
    override def encode(value: String, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(value), 0)
  }

  implicit object BooleanEncoder extends TupleEncoder[Boolean] {
    override def encode(value: Boolean): Tuple = new Tuple().add(value)
    override def encode(value: Boolean, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(value), 0)
  }

  implicit def anyValEncoder[W, U](
      implicit unwrapped: Unwrapped.Aux[W, U],
      encoder: TupleEncoder[U]): TupleEncoder[W] = {
    encoder.contramap[W](unwrapped.unwrap)
  }

  implicit def optionEncoder[A](implicit ev: TupleEncoder[A]): TupleEncoder[Option[A]] =
    new TupleEncoder[Option[A]] {
      override def encode(value: Option[A]): Tuple =
        value.fold(new Tuple().addObject(null))(ev.encode)
      override def encode(value: Option[A], acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
        value.fold((acc, preceedingNulls + 1))(ev.encode(_, acc, preceedingNulls))
    }

  // todo maybe deserialize it through option?
  // fixme it won't work for List[None]
  implicit def listEncoder[A](implicit ev: TupleEncoder[A]): TupleEncoder[List[A]] =
    new TupleEncoder[List[A]] {
      override def encode(value: List[A]): Tuple =
        encode(value, new Tuple(), 0)._1

      override def encode(value: List[A], acc: Tuple, preceedingNulls: Int): (Tuple, Int) = {
        if (value.isEmpty) (acc, preceedingNulls + 1)
        else {
          val listTuple = value.foldLeft(new Tuple())((acc, a) => ev.encode(a, acc, 0)._1)
          (addNulls(acc, preceedingNulls).add(listTuple), 0)
        }
      }
    }

  private def encodeInt(value: Int): Array[Byte] = {
    val output = new Array[Byte](4)
    ByteBuffer.wrap(output).putInt(value)
    output
  }
}

object EncodersHelpers {
  def addNulls(tuple: Tuple, preceedingNulls: Int): Tuple = {
    var acc = tuple
    var i = 0
    while (i < preceedingNulls) {
      acc = acc.addObject(null)
      i += 1
    }
    acc
  }
}
