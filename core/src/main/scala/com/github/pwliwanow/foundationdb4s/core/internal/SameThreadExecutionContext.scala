package com.github.pwliwanow.foundationdb4s.core.internal
import scala.concurrent.ExecutionContext

// It should only be used for actions that are known to be non-blocking and non-throwing exceptions
private[foundationdb4s] object SameThreadExecutionContext extends ExecutionContext {
  override def execute(runnable: Runnable): Unit = runnable.run()
  override def reportFailure(cause: Throwable): Unit = {
    throw new IllegalStateException("Exception occurred in SameThreadExecutionContext", cause)
  }
}
