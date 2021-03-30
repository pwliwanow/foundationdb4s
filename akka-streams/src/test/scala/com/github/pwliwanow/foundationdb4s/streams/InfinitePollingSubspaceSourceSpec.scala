package com.github.pwliwanow.foundationdb4s.streams
import java.time.Instant

import akka.stream.scaladsl.Sink
import cats.syntax.all._
import com.github.pwliwanow.foundationdb4s.core.FriendEntity

import scala.concurrent.duration._

class InfinitePollingSubspaceSourceSpec extends FoundationDbStreamsSpec {
  private val entity =
    FriendEntity(
      ofUserId = 1L,
      addedAt = Instant.parse("2018-08-03T10:15:30.00Z"),
      friendId = 10L,
      friendName = "John")

  it should "continuously stream data from the given subspace" in {
    val numberOfElements = 100
    val (firstHalf, secondHalf) =
      (1 to numberOfElements).map(entityFromInt).toList.splitAt(numberOfElements / 2)
    val source = InfinitePollingSubspaceSource.from(typedSubspace, database, 50.millis)
    insert(firstHalf)
    val futureResult = source.take(numberOfElements.toLong).runWith(Sink.seq)
    // wait for stream to initialize and stream first part
    Thread.sleep(800)
    insert(secondHalf)
    val result = futureResult.await
    assert(result === (firstHalf ++ secondHalf))
  }

  private def insert(xs: List[FriendEntity]): Unit = {
    xs.traverse(typedSubspace.set).transact(database).await
    ()
  }

  private def entityFromInt(i: Int): FriendEntity = {
    entity.copy(addedAt = entity.addedAt.plusSeconds(i.toLong), friendId = entity.friendId + i)
  }
}
