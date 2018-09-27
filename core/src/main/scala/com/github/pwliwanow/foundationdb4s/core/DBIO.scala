package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.CompletableFuture

import cats.{Monad, StackSafeMonad}
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Versionstamp

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
    transactVersionstamped(transactor).map { case (x, _) => x }(transactor.ec)
  }

  def transactVersionstamped(
      transactor: Transactor,
      userVersion: Int = 0): Future[(A, Versionstamp)] = {
    implicit val ec = transactor.ec
    var futureVersionstamp: Future[Versionstamp] = null
    val futureRes: Future[A] = transactor.db
      .runAsync(
        (tx: Transaction) => {
          futureVersionstamp = tx.getVersionstamp.toScala.map(Versionstamp.complete(_, userVersion))
          run(tx, transactor.ec).toJava.asInstanceOf[CompletableFuture[A]]
        },
        transactor.ec
      )
      .toScala
    futureRes.zip(futureVersionstamp)
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
