package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.{CompletableFuture, CompletionException}

import cats.{Monad, StackSafeMonad}
import com.apple.foundationdb.tuple.Versionstamp
import com.apple.foundationdb.{Database, Transaction, TransactionContext}
import com.github.pwliwanow.foundationdb4s.core.internal.CompletableFutureHolder._
import com.github.pwliwanow.foundationdb4s.core.internal.TransactionalM

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.Try

final case class DBIO[A] private (private val underlying: TransactionalM[Transaction, A]) {

  def map[B](f: A => B): DBIO[B] = {
    DBIO(underlying.map(f))
  }

  def flatMap[B](f: A => DBIO[B]): DBIO[B] = {
    DBIO(underlying.flatMap(a => f(a).underlying))
  }

  /** Runs DBIO within provided [[TransactionContext]].
    *
    * Depending on the type of context, this may execute the supplied function multiple times
    * if client is unable to determine whether a transaction succeeded.
    * For more information see:
    * https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results
    *
    * @return a [[Future]] that will contain a value returned by running this DBIO
    */
  def transact(context: TransactionContext)(implicit ec: ExecutionContextExecutor): Future[A] = {
    transactJava(context).toScala.recoverWith {
      case e: CompletionException if e.getCause != null =>
        Future.failed(e.getCause)
    }
  }

  /** Runs DBIO within provided [[TransactionContext]].
    *
    * Depending on the type of context, this may execute the supplied function multiple times
    * if client is unable to determine whether a transaction succeeded.
    * For more information see:
    * https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results
    *
    * @return a [[CompletableFuture]] that will contain a value returned by running this DBIO.
    */
  def transactJava(context: TransactionContext): CompletableFuture[A] = {
    context.runAsync(tx => underlying.run(tx))
  }

  /** Runs a transactional DBIO with retry logic in a non-blocking way.
    *
    * In some cases client can be unable to determine whether a transaction succeeded.
    * In these cases, your transaction may be executed twice.
    * For more information see:
    * https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results
    *
    * Any error encountered when executing DBIO will be set on the resulting Future.
    *
    * @return a [[Future]] that will contain a tuple of value returned by running this DBIO and
    *         an optional [[Versionstamp]].
    *         Returned [[Versionstamp]] will be empty if executing this DBIO did not modify the database
    *         (e.g. when running this function on `DBIO.pure("value")`).
    *         For DBIO that modified the database, [[Versionstamp]] will be equal to the versionstamp used
    *         by any versionstamp operations in this DBIO.
    */
  def transactVersionstamped(database: Database)(
      implicit ec: ExecutionContextExecutor): Future[(A, Option[Versionstamp])] = {
    transactVersionstamped(database, userVersion = 0)
  }

  /** Runs a transactional DBIO with retry logic in a non-blocking way.
    *
    * In some cases client can be unable to determine whether a transaction succeeded.
    * In these cases, your transaction may be executed twice.
    * For more information see:
    * https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results
    *
    * Any error encountered when executing DBIO will be set on the resulting Future.
    *
    * @return a [[Future]] that will contain a tuple of value returned by running this DBIO and
    *         an optional [[Versionstamp]].
    *         Returned [[Versionstamp]] will be empty if executing this DBIO did not modify the database
    *         (e.g. when running this function on `DBIO.pure("value")`).
    *         For DBIO that modified the database, [[Versionstamp]] will be equal to the versionstamp used
    *         by any versionstamp operations in this DBIO.
    */
  def transactVersionstamped(database: Database, userVersion: Int)(
      implicit ec: ExecutionContextExecutor): Future[(A, Option[Versionstamp])] = {
    transactVersionstampedJava(database, userVersion).toScala.recoverWith {
      case e: CompletionException if e.getCause != null =>
        Future.failed(e.getCause)
    }
  }

  /** Runs a transactional DBIO with retry logic in a non-blocking way.
    *
    * In some cases client can be unable to determine whether a transaction succeeded.
    * In these cases, your transaction may be executed twice.
    * For more information see:
    * https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results
    *
    * Any error encountered when executing DBIO will be set on the resulting Future.
    *
    * @return a [[CompletableFuture]] that will contain a tuple of value returned by running this DBIO and
    *         an optional [[Versionstamp]].
    *         Returned [[Versionstamp]] will be empty if executing this DBIO did not modify the database
    *         (e.g. when running this function on `DBIO.pure("value")`).
    *         For DBIO that modified the database, [[Versionstamp]] will be equal to the versionstamp used
    *         by any versionstamp operations in this DBIO.
    */
  def transactVersionstampedJava(
      database: Database,
      userVersion: Int): CompletableFuture[(A, Option[Versionstamp])] = {
    val versionstampPromise = new CompletableFuture[Versionstamp]()
    val res: CompletableFuture[A] = database.runAsync { tx =>
      versionstampPromise.completeWith {
        tx.getVersionstamp.thenApply[Versionstamp] { bytes =>
          Versionstamp.complete(bytes, userVersion)
        }
      }
      underlying.run(tx)
    }
    val futureMaybeVersionstamp =
      versionstampPromise.handle[Option[Versionstamp]]((v, ex) => if (ex == null) Some(v) else None)
    res.zip(futureMaybeVersionstamp)
  }
}

object DBIO {
  def pure[A](value: A): DBIO[A] = {
    DBIO(TransactionalM.pure[Transaction, A](value))
  }

  def failed[A](ex: Throwable): DBIO[A] = {
    DBIO(TransactionalM.failed[Transaction, A](ex))
  }

  def fromTransactionToPromise[A](f: Transaction => CompletableFuture[A]): DBIO[A] = {
    DBIO(TransactionalM.fromTransactionToPromise[Transaction, A](f))
  }

  def fromTransactionToTry[A](f: Transaction => Try[A]): DBIO[A] = {
    DBIO(TransactionalM.fromTransactionToTry[Transaction, A](f))
  }

  implicit val dbioMonad: Monad[DBIO] = new Monad[DBIO] with StackSafeMonad[DBIO] {
    override def pure[A](x: A): DBIO[A] = DBIO.pure(x)
    override def flatMap[A, B](fa: DBIO[A])(f: A => DBIO[B]): DBIO[B] = fa.flatMap(f)
  }

}
