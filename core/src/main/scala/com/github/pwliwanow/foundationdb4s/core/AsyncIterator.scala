package com.github.pwliwanow.foundationdb4s.core

import com.apple.foundationdb.async.{AsyncIterator => FdbAsyncIterator}

import scala.concurrent.Future
import scala.compat.java8.FutureConverters._

/** Similar to [[FdbAsyncIterator]], except:
  * - it exposes [[scala.concurrent.Future]] based api instead of [[java.util.concurrent.CompletableFuture]]
  * based one
  * - it does not extend [[java.util.Iterator]], so it also does not expose blocking `hasNext()` operation
  */
trait AsyncIterator[A] {

  def onHasNext(): Future[Boolean]

  /** Similar to [[FdbAsyncIterator.hasNext]] method.
    * It will not block if [[onHasNext()]] was called and resulting future has completed.
    *
    * @return the next element in the sequence, blocking if necessary.
    * @throws NoSuchElementException if the sequence has been exhausted.
    */
  def next(): A

  /** Cancels any outstanding asynchronous work associated with this [[AsyncIterator]]. */
  def cancel(): Unit

}

object AsyncIterator {
  implicit class FdbAsnycIteratorHolder[A](val fdbIterator: FdbAsyncIterator[A]) extends AnyVal {
    def toScala: AsyncIterator[A] = apply(fdbIterator)
  }

  def apply[A](fdbAsyncIterator: FdbAsyncIterator[A]): AsyncIterator[A] = {
    new FutureBasedAsyncIterator[A](fdbAsyncIterator)
  }
}

private class FutureBasedAsyncIterator[A](underlying: FdbAsyncIterator[A])
    extends AsyncIterator[A] {
  override def onHasNext(): Future[Boolean] =
    underlying.onHasNext().toScala.asInstanceOf[Future[Boolean]]

  /** Similar to [[FdbAsyncIterator.hasNext]] method.
    * It will not block if [[onHasNext()]] was called and resulting future has completed.
    *
    * @return the next element in the sequence, blocking if necessary.
    * @throws NoSuchElementException if the sequence has been exhausted.
    */
  override def next(): A = underlying.next()

  /** Cancels any outstanding asynchronous work associated with this [[AsyncIterator]]. */
  override def cancel(): Unit = underlying.cancel()
}
