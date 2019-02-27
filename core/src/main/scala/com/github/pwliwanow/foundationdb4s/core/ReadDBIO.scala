package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.{CompletableFuture, CompletionException}

import cats.{Monad, StackSafeMonad}
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

}
