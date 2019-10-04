package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.{CompletableFuture, CompletionException}
import java.util.function.{Function => JF}

import cats.{Applicative, Monad, Parallel, StackSafeMonad, ~>}
import com.apple.foundationdb.{ReadTransaction, ReadTransactionContext, Transaction}
import com.github.pwliwanow.foundationdb4s.core.internal.TransactionalM

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

final case class ReadDBIO[+A] private (private val underlying: TransactionalM[ReadTransaction, A]) {

  def map[B](f: A => B): ReadDBIO[B] = {
    ReadDBIO(underlying.map(f))
  }

  def flatMap[B](f: A => ReadDBIO[B]): ReadDBIO[B] = {
    ReadDBIO(underlying.flatMap(a => f(a).underlying))
  }

  def toDBIO: DBIO[A] = {
    DBIO.fromTransactionToPromise((tx: Transaction) => underlying.run(tx))
  }

  def transact(readContext: ReadTransactionContext)(
      implicit ec: ExecutionContextExecutor): Future[A] = {
    transactJava(readContext).toScala
      .recoverWith {
        case e: CompletionException if e.getCause != null =>
          Future.failed(e.getCause)
      }
  }

  def transactJava[B >: A](readContext: ReadTransactionContext): CompletableFuture[B] = {
    readContext.readAsync(tx => underlying.run(tx))
  }
}

object ReadDBIO {
  val unit: ReadDBIO[Unit] = pure(())

  def failed[A](value: Throwable): ReadDBIO[A] =
    ReadDBIO(TransactionalM.failed[ReadTransaction, A](value))

  def pure[A](value: A): ReadDBIO[A] = ReadDBIO(TransactionalM.pure(value))

  def fromTransactionToPromise[A](f: ReadTransaction => CompletableFuture[A]): ReadDBIO[A] = {
    ReadDBIO(TransactionalM.fromTransactionToPromise(f))
  }

  def fromTransactionToTry[A](f: ReadTransaction => Try[A]): ReadDBIO[A] = {
    ReadDBIO(TransactionalM.fromTransactionToTry[ReadTransaction, A](f))
  }

  implicit val readDbioMonad: Monad[ReadDBIO] = new Monad[ReadDBIO] with StackSafeMonad[ReadDBIO] {
    override def pure[A](x: A): ReadDBIO[A] = ReadDBIO.pure(x)
    override def flatMap[A, B](fa: ReadDBIO[A])(f: A => ReadDBIO[B]): ReadDBIO[B] = fa.flatMap(f)
  }

  // Newtype encoding inspired by cats IO and [[https://github.com/alexknvl/newtypes]]
  object Par {
    type Base
    trait Tag extends Any
    type Type[+A] <: Base with Tag

    def apply[A](fa: ReadDBIO[A]): Type[A] =
      fa.asInstanceOf[Type[A]]

    def unwrap[A](fa: Type[A]): ReadDBIO[A] =
      fa.asInstanceOf[ReadDBIO[A]]
  }
  type Par[+A] = Par.Type[A]

  implicit val parApplicative: Applicative[Par] = new Applicative[Par] {
    override def pure[A](x: A): Par[A] =
      Par(ReadDBIO.pure(x))
    override def map2[A, B, Z](fa: Par[A], fb: Par[B])(f: (A, B) => Z): Par[Z] = {
      val dbio = ReadDBIO.fromTransactionToPromise { tx =>
        val pa = Par.unwrap(fa).underlying.run(tx)
        val pb = Par.unwrap(fb).underlying.run(tx)
        val jf: JF[A, CompletableFuture[Z]] = a => {
          pb.thenApply[Z] { b =>
            f(a, b)
          }
        }
        pa.thenComposeAsync[Z](jf, tx.getExecutor)
      }
      Par(dbio)
    }
    override def ap[A, B](ff: Par[A => B])(fa: Par[A]): Par[B] =
      map2(ff, fa)(_(_))
    override def product[A, B](fa: Par[A], fb: Par[B]): Par[(A, B)] =
      map2(fa, fb)((_, _))
  }

  implicit val readDbioParallel: Parallel[ReadDBIO, Par] = new Parallel[ReadDBIO, Par] {
    override def applicative: Applicative[Par] = parApplicative
    override def monad: Monad[ReadDBIO] = readDbioMonad
    override def sequential: Par ~> ReadDBIO = {
      new ~>[Par, ReadDBIO] {
        override def apply[A](fa: Par[A]): ReadDBIO[A] = Par.unwrap(fa)
      }
    }
    override def parallel: ReadDBIO ~> Par = {
      new ~>[ReadDBIO, Par] {
        override def apply[A](fa: ReadDBIO[A]): Par[A] = Par(fa)
      }
    }
  }

}
