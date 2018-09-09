package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.CompletableFuture

import cats.{Monad, StackSafeMonad}
import com.apple.foundationdb.Transaction

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.compat.java8.FutureConverters._

final case class DBIO[+A](
    private[foundationdb4s] val run: (Transaction, ExecutionContextExecutor) => Future[A]) {
  def map[B](f: A => B): DBIO[B] = DBIO {
    case (tx, ec) =>
      run(tx, ec).map(f)(ec)
  }

  def flatMap[B](f: A => DBIO[B]): DBIO[B] = DBIO {
    case (tx, ec) =>
      run(tx, ec).flatMap(a => f(a).run(tx, ec))(ec)
  }

  def transact(transactor: Transactor): Future[A] = {
    transactor.db
      .runAsync(
        tx => run(tx, transactor.ec).toJava.asInstanceOf[CompletableFuture[A]],
        transactor.ec)
      .toScala
  }
}

object DBIO {
  def failed[A](value: Throwable): DBIO[A] = DBIO[A] {
    case (_, _) => Future.failed[A](value)
  }

  def pure[A](value: A): DBIO[A] = DBIO {
    case (_, _) => Future.successful(value)
  }

  implicit val dbioMonad: Monad[DBIO] = new Monad[DBIO] with StackSafeMonad[DBIO] {
    override def pure[A](x: A): DBIO[A] = DBIO.pure(x)
    override def flatMap[A, B](fa: DBIO[A])(f: A => DBIO[B]): DBIO[B] = fa.flatMap(f)
  }
}
