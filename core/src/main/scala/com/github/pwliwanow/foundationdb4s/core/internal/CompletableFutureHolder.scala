package com.github.pwliwanow.foundationdb4s.core.internal
import java.util.concurrent.CompletableFuture

import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

private[foundationdb4s] object CompletableFutureHolder {
  implicit def toComplFutureHolder[A](value: CompletableFuture[A]): CompletableFutureHolder[A] = {
    new CompletableFutureHolder[A](value)
  }
}

private[foundationdb4s] class CompletableFutureHolder[A](val value: CompletableFuture[A])
    extends AnyVal {
  def completeWithTry(result: Try[A]): Boolean = result match {
    case Success(v) => value.complete(v)
    case Failure(t) => value.completeExceptionally(t)
  }

  def completeWith(other: CompletableFuture[A]): Unit = {
    other.handle[Unit] { (x, e) =>
      if (e eq null) {
        value.complete(x)
        ()
      } else {
        value.completeExceptionally(e)
        ()
      }
    }
    ()
  }

  def toPromise: Promise[A] = {
    val result = Promise[A]()
    result.future.onComplete {
      case Success(v)  => value.complete(v)
      case Failure(ex) => value.completeExceptionally(ex)
    }(SameThreadExecutionContext)
    value.handle[Unit] { (v, ex) =>
      // note that checking for completeness here is only on best effort basis,
      // that's why later `result.complete` is wrapped in Try
      if (!result.isCompleted) {
        val tryResult = if (ex eq null) Success(v) else Failure(ex)
        Try(result.complete(tryResult))
      }
      ()
    }
    result
  }

  def zip[B](other: CompletableFuture[B]): CompletableFuture[(A, B)] = {
    value.thenCompose(a => other.thenApply(b => (a, b)))
  }
}
