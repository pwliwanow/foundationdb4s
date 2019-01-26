package com.github.pwliwanow.foundationdb4s.streams
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.pwliwanow.foundationdb4s.core.FoundationDbSpec

import scala.concurrent.ExecutionContextExecutor

abstract class FoundationDbStreamsSpec
    extends TestKit(ActorSystemHolder.system)
    with FoundationDbSpec {
  implicit override def ec: ExecutionContextExecutor = system.dispatcher
}

object ActorSystemHolder {
  val system: ActorSystem = ActorSystem("test-system")
}
