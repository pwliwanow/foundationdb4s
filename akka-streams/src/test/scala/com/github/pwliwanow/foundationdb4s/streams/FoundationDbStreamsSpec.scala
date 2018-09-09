package com.github.pwliwanow.foundationdb4s.streams
import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.pwliwanow.foundationdb4s.core.{FoundationDbSpec, Transactor}

import scala.concurrent.ExecutionContextExecutor

abstract class FoundationDbStreamsSpec
    extends TestKit(ActorSystemHolder.system)
    with FoundationDbSpec {
  implicit override def ec: ExecutionContextExecutor = system.dispatcher
  override lazy val testTransactor: Transactor = Transactor(520)(ec)
}

object ActorSystemHolder {
  val system: ActorSystem = ActorSystem("test-system")
}
