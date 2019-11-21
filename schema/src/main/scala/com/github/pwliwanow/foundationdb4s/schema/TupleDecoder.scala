package com.github.pwliwanow.foundationdb4s.schema
import java.nio.ByteBuffer

import com.apple.foundationdb.tuple.Tuple
import shapeless.Unwrapped

import scala.collection.mutable.ListBuffer
import scala.util.Try

/** A type class that provides conversion from [[Tuple]] to type [[A]].
  *
  * All implementations must respect backwards compatibility.
  */
trait TupleDecoder[A] { self =>

  /** Decodes [[Tuple]] as [[A]]. */
  def decode(tuple: Tuple): A

  /** Decodes value from [[Tuple]] at the specified index as [[A]]. */
  def decode(tuple: Tuple, index: Int): A

  /** Creates new instance of [[TupleDecoder]] by applying function `f`
    * to a value that was obtained by deserializing given tuple as an [[A]].
    */
  def map[B](f: A => B): TupleDecoder[B] = new TupleDecoder[B] {
    override def decode(tuple: Tuple): B = f(self.decode(tuple))
    override def decode(tuple: Tuple, index: Int): B = f(self.decode(tuple, index))
  }
}

object TupleDecoder extends BasicDecodersProtocol {
  /** Derives [[TupleDecoder]] for [[A]] type parameter.
    *
    * Derivation will succeed only if there exist implicit
    * decoders for all members of [[A]].
    */
  def derive[A](implicit dec: DerivedDecoder[A]): TupleDecoder[A] = dec
}

trait BasicDecodersProtocol {
  implicit object IntDecoder extends TupleDecoder[Int] {
    override def decode(tuple: Tuple): Int = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): Int = decodeInt(tuple.getBytes(index))
  }

  implicit object LongDecoder extends TupleDecoder[Long] {
    override def decode(tuple: Tuple): Long = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): Long = tuple.getLong(index)
  }

  implicit object StringDecoder extends TupleDecoder[String] {
    override def decode(tuple: Tuple): String = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): String = tuple.getString(index)
  }

  implicit object BooleanDecoder extends TupleDecoder[Boolean] {
    override def decode(tuple: Tuple): Boolean = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): Boolean = tuple.getBoolean(index)
  }

  implicit def anyValDecoder[W, U](
      implicit unwrapped: Unwrapped.Aux[W, U],
      ev: TupleDecoder[U]): TupleDecoder[W] = {
    ev.map(unwrapped.wrap)
  }

  implicit def optionDecoder[A](implicit ev: TupleDecoder[A]): TupleDecoder[Option[A]] =
    new TupleDecoder[Option[A]] {
      override def decode(tuple: Tuple): Option[A] = decode(tuple, 0)

      override def decode(tuple: Tuple, index: Int): Option[A] = {
        Try(Option(ev.decode(tuple, index))).recover {
          case _: NullPointerException      => None
          case _: IndexOutOfBoundsException => None
        }.get
      }
    }

  implicit def listDecoder[A](implicit ev: TupleDecoder[A]): TupleDecoder[List[A]] =
    new TupleDecoder[List[A]] {
      override def decode(tuple: Tuple): List[A] = decode(tuple, 0)
      override def decode(tuple: Tuple, index: Int): List[A] = {
        val result = Try {
          val nested = tuple.getNestedTuple(index)
          val acc = ListBuffer.empty[A]
          for (i <- 0 until nested.size()) {
            acc += ev.decode(nested, i)
          }
          acc.toList
        }
        result.recover {
          case _: NullPointerException      => List.empty
          case _: IndexOutOfBoundsException => List.empty
        }.get
      }
    }

  private def decodeInt(value: Array[Byte]): Int = {
    if (value.length != 4) throw new IllegalArgumentException("Array must be of size 4")
    ByteBuffer.wrap(value).getInt
  }
}
