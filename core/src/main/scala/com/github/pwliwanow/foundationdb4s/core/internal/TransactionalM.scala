package com.github.pwliwanow.foundationdb4s.core.internal
import java.util.concurrent.CompletableFuture

import com.github.pwliwanow.foundationdb4s.core.internal.CompletableFutureHolder._
import com.github.pwliwanow.foundationdb4s.core.internal.Or3.{Left, Middle, Right}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

private[foundationdb4s] abstract class TransactionalM[Tx, A] {
  import TransactionalM._

  private[foundationdb4s] final def map[B](f: A => B): TransactionalM[Tx, B] = {
    flatMap(a => Pure(f(a)))
  }

  private[foundationdb4s] final def flatMap[B](
      f: A => TransactionalM[Tx, B]): TransactionalM[Tx, B] = {
    FlatMap(this, f)
  }

  private[foundationdb4s] final def run(tx: Tx): CompletableFuture[A] = {
    def loop(dbio: TransactionalM[Tx, _]): CompletableFuture[A] = {
      dbio.resume(tx) match {
        case Right(v) =>
          val result = new CompletableFuture[A]()
          result.completeWithTry(v.asInstanceOf[Try[A]])
          result
        case Middle(v) => v.asInstanceOf[CompletableFuture[A]]
        case Left(f) =>
          f.thenCompose[A](x => loop(x))
      }
    }
    loop(this)
  }

  @tailrec
  private final def resume(
      tx: Tx): Or3[CompletableFuture[TransactionalM[Tx, A]], CompletableFuture[A], Try[A]] = {
    this match {
      case Pure(v)         => Right(Success(v))
      case RaiseError(ex)  => Right(Failure(ex))
      case TryAction(f)    => Right(f(tx))
      case FutureAction(f) => Middle(f(tx))
      case FlatMap(c, f) =>
        c match {
          case Pure(v)        => f(v).resume(tx)
          case RaiseError(ex) => Right(Failure(ex))
          case TryAction(g) =>
            g(tx) match {
              case Success(v)  => f(v).resume(tx)
              case Failure(ex) => Right(Failure(ex))
            }
          case FutureAction(toF) =>
            Left(toF(tx).thenApply(x => f(x)))
          case FlatMap(d, g) =>
            d.flatMap(dd => g(dd).flatMap(f)).resume(tx)
        }
    }
  }

}

private[foundationdb4s] object TransactionalM {

  private[foundationdb4s] def pure[Tx, A](value: A): TransactionalM[Tx, A] = {
    Pure(value)
  }

  private[foundationdb4s] def failed[Tx, A](value: Throwable): TransactionalM[Tx, A] = {
    RaiseError[Tx, A](value)
  }

  private[foundationdb4s] def fromTransactionToPromise[Tx, A](
      f: Tx => CompletableFuture[A]): TransactionalM[Tx, A] = {
    FutureAction(f)
  }

  private[foundationdb4s] def fromTransactionToTry[Tx, A](
      f: Tx => Try[A]): TransactionalM[Tx, A] = {
    TryAction(f)
  }

  private final case class Pure[Tx, A](value: A) extends TransactionalM[Tx, A]

  private final case class FutureAction[Tx, A](f: Tx => CompletableFuture[A])
      extends TransactionalM[Tx, A]

  private final case class TryAction[Tx, A](f: Tx => Try[A]) extends TransactionalM[Tx, A]

  private final case class FlatMap[Tx, S, A](
      source: TransactionalM[Tx, S],
      f: S => TransactionalM[Tx, A])
      extends TransactionalM[Tx, A]

  private final case class RaiseError[Tx, A](ex: Throwable) extends TransactionalM[Tx, A]

}
