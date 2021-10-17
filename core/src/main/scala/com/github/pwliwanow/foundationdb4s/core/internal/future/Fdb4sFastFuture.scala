package com.github.pwliwanow.foundationdb4s.core.internal.future

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

private[foundationdb4s] object Fdb4sFastFuture {

  implicit def toFutureOps[A](value: Future[A]): Fdb4sFutureOps[A] = new Fdb4sFutureOps(value)

  class Fdb4sFutureOps[A](val value: Future[A]) extends AnyVal {
    def toFastFuture = new Fdb4sFastFuture(value)
  }

  class Fdb4sFastFuture[A](val underlying: Future[A]) extends AnyVal {
    def map[B](f: A => B): Future[B] = {
      underlying.value match {
        case Some(Success(value)) =>
          Future.successful(f(value))
        case Some(Failure(e)) =>
          Future.failed(e)
        case None =>
          underlying.map(f)(scala.concurrent.ExecutionContext.parasitic)
      }
    }

    def flatMap[B](f: A => Future[B]): Future[B] = {
      underlying.value match {
        case Some(Success(value)) =>
          f(value)
        case Some(Failure(e)) =>
          Future.failed(e)
        case None =>
          underlying.flatMap(f)(scala.concurrent.ExecutionContext.parasitic)
      }
    }

    def onComplete[B](f: Try[A] => B): Unit = {
      underlying.value match {
        case Some(x) => f(x)
        case None =>
          underlying.onComplete(f)(scala.concurrent.ExecutionContext.parasitic)
      }
    }

    def recover(f: PartialFunction[Throwable, A]): Future[A] = {
      transform(_.recover(f))
    }

    def recoverWith(f: PartialFunction[Throwable, Future[A]]): Future[A] = {
      underlying.value match {
        case Some(Success(_)) =>
          underlying
        case Some(Failure(e)) if f.isDefinedAt(e) =>
          f(e)
        case _ =>
          underlying.recoverWith(f)(scala.concurrent.ExecutionContext.parasitic)
      }
    }

    def transform[B](f: Try[A] => Try[B]): Future[B] = {
      underlying.value match {
        case Some(x) =>
          Future.fromTry(f(x))
        case None =>
          underlying.transform(f)(scala.concurrent.ExecutionContext.parasitic)
      }
    }
  }

}
