package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.CompletableFuture

import cats.{Monad, StackSafeMonad}
import com.apple.foundationdb.ReadTransaction

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.compat.java8.FutureConverters._

final case class ReadDBIO[+A](
    private[foundationdb4s] val run: (ReadTransaction, ExecutionContextExecutor) => Future[A]) {
  def map[B](f: A => B): ReadDBIO[B] = ReadDBIO {
    case (tx, ec) =>
      run(tx, ec).map(f)(ec)
  }

  def flatMap[B](f: A => ReadDBIO[B]): ReadDBIO[B] = ReadDBIO {
    case (tx, ec) =>
      run(tx, ec).flatMap(a => f(a).run(tx, ec))(ec)
  }

  def flatMap[B](f: A => DBIO[B]): DBIO[B] = ReadDBIO.toDBIO(this).flatMap(f)

  def transact(transactor: Transactor): Future[A] = {
    transactor.db
      .readAsync(
        tx => run(tx, transactor.ec).toJava.asInstanceOf[CompletableFuture[A]],
        transactor.ec)
      .toScala
  }
}

object ReadDBIO {
  def failed[A](value: Throwable): ReadDBIO[A] = ReadDBIO[A] {
    case (_, _) => Future.failed[A](value)
  }

  def pure[A](value: A): ReadDBIO[A] = ReadDBIO {
    case (_, _) => Future.successful(value)
  }

  implicit def toDBIO[A](readDbio: ReadDBIO[A]): DBIO[A] = DBIO {
    case (tx, ec) => readDbio.run(tx, ec)
  }

  implicit val readDbioMonad: Monad[ReadDBIO] = new Monad[ReadDBIO] with StackSafeMonad[ReadDBIO] {
    override def pure[A](x: A): ReadDBIO[A] = ReadDBIO.pure(x)
    override def flatMap[A, B](fa: ReadDBIO[A])(f: A => ReadDBIO[B]): ReadDBIO[B] = fa.flatMap(f)
  }
}
