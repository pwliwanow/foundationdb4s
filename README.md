# foundationdb4s

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.pwliwanow.foundationdb4s/foundationdb4s-core_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.pwliwanow.foundationdb4s/foundationdb4s-core_2.12)
[![Build Status](https://travis-ci.org/pwliwanow/foundationdb4s.svg?branch=master)](https://travis-ci.org/pwliwanow/foundationdb4s)
[![codecov](https://codecov.io/gh/pwliwanow/foundationdb4s/branch/master/graph/badge.svg)](https://codecov.io/gh/pwliwanow/foundationdb4s)

foundationdb4s is a wrapper for [FoundationDB](https://github.com/apple/foundationdb) Java client.
It aims to be type-safe and idiomatic for Scala.

```scala
implicit val ec = scala.concurrent.ExecutionContext.global
val database: Database = FDB.selectAPIVersion(600).open(null, ec)

final case class Book(isbn: String, title: String, publishedOn: LocalDate)

val booksSubspace = new TypedSubspace[Book, String] {
  override val subspace: Subspace = new Subspace(Tuple.from("books"))
  override def toKey(entity: Book): String = entity.isbn
  override def toRawValue(entity: Book): Array[Byte] = {
    Tuple.from(entity.title, entity.publishedOn.toString).pack
  }
  override def toTupledKey(key: String): Tuple = Tuple.from(key)
  override def toKey(tupledKey: Tuple): String = tupledKey.getString(0)
  override def toEntity(key: String, value: Array[Byte]): Book = {
    val tupledValue = Tuple.fromBytes(value)
    val publishedOn = LocalDate.parse(tupledValue.getString(1))
    Book(isbn = key, title = tupledValue.getString(0), publishedOn = publishedOn)
  }
}

val dbio: DBIO[Option[Book]] = for {
  _ <- booksSubspace.set(Book("978-0451205766", "The Godfather", LocalDate.parse("2002-03-01")))
  maybeBook <- booksSubspace.get("978-0451205766").toDBIO
} yield maybeBook

val maybeBook: Future[Option[Book]] = dbio.transact(database)
```

The library is still incomplete and does not cover everything official client does.

If you:
- think some functionality is missing
- find a bug 
- feel that API does not "feel right"

please create an issue.

## Quickstart with sbt
To get started you can add the following dependencies to your project:
```scala
val fdb4sVersion = "0.7.0"

libraryDependencies ++= Seq(
  "com.github.pwliwanow.foundationdb4s" %% "foundationdb4s-core" % fdb4sVersion,
  "com.github.pwliwanow.foundationdb4s" %% "foundationdb4s-akka-streams" % fdb4sVersion
)
```

## Integrations
- Cats - Monad instances are provided in companion objects for `DBIO` and `ReadDBIO`
- Akka Streams `Source` implementation (`akka-streams` module)

## Basic abstractions
Modifying data within a `TypedSubspace` (`clear` and `set` operations) returns `DBIO[_]` monad.

Reading from a `TypedSubspace` (`get` and `getRange` operations) returns `ReadDBIO[_]` monad.
`ReadDBIO[_]` provides method `.toDBIO` for converting `ReadDBIO[_]` into `DBIO[_]`.

## Versionstamps 
Versionstamp consists of "transaction" version and of a user version.

Transaction version is usually assigned by the database in such a way that 
all transactions receive a different version that is consistent with a serialization 
order of the transactions within the database. 
This also implies that the transaction version of newly committed transactions will 
be monotonically increasing over time.
Note that transaction version will be assigned during commit, 
which implies that it is not possible to use/get "current" transaction version inside the transaction itself. 

User version should be set by the client. 
It allows the user to use this class to impose a total order of items across multiple 
transactions in the database in a consistent and conflict-free way.

More information in [FoundationDB Javadoc](https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/tuple/Versionstamp.html).

### Working with Versionstamps

foundationdb4s supports working with keys or values that contain versionstamps 
by providing `SubspaceWithVersionstampedKeys` and `SubspaceWithVersionstampedValues`.
Compared to `TypedSubspace`, those require additional method to be implemented: `extractVersionstamp`.

To obtain versionstamp which was used by any versionstamp operations in this `DBIO`, 
use `transactVersionstamped` instead of `transact`.

Note that if the given `DBIO` did not modify the database, returned `Versionstamp` will be empty.

```scala
implicit val ec = scala.concurrent.ExecutionContext.global
val database: Database = FDB.selectAPIVersion(600).open(null, ec)

case class EventKey(eventType: String, versionstamp: Versionstamp)
case class Event(key: EventKey, content: Array[Byte])

val eventsSubspace = new SubspaceWithVersionstampedKeys[Event, EventKey] {
  override val subspace: Subspace = new Subspace(Tuple.from("events"))
  override def toKey(entity: Event): EventKey = entity.key
  override def toRawValue(entity: Event): Array[Byte] = event.content
  override def toTupledKey(key: EventKey): Tuple = Tuple.from(key.eventType, key.versiostamp)
  override def toKey(tupledKey: Tuple): EventKey = {
    EventKey(tupledKey.getString(0), tupledKey.getVersiostamp(1))
  }
  override def toEntity(key: EventKey, value: Array[Byte]): Event = Event(key, value)
  override def extractVersionstamp(key: EventKey): Versionstamp = key.versionstamp
}

val event = Event(
  key = EventKey("UserAdded", Versionstamp.incomplete(0)), 
  content = Tuple.from("""{ "name": "John Smith" }""").pack)

// save new event
val setDbio: DBIO[Unit] = eventsSubspace.set(event)
val completedVersionstamp: Versionstamp = 
  Await.result(
    setDbio
      .transactVersionstamped(database, userVersion = 0)
      .map { case (_, Some(versionstamp)) => versionstamp },
    Duration.Inf)

// update previously persisted event
val updatedEvent = event.copy(key = event.key.copy(versionstamp = completedVersionstamp))
val updateDbio = eventsSubspace.set(updatedEvent)

updateDbio.transact(database)
``` 

## Watches
When application needs to monitor changes done to a given key, it can periodically read the key or it can use _watches_.
Watches are created for a given key, and return a `Promise` that will be completed, once the value for the key changes.

Note that there is limited number of watches that can be active for each database connection (by default 10,000),
so watches that are no longer needed should be canceled.
Once the number is exceeded creating new watches will fail.

```scala
// reusing first example
final case class Book(isbn: String, title: String, publishedOn: LocalDate)
val booksSubspace: TypedSubspace[Book, String] = ???

val key = "978-0451205766"
val dbio: DBIO[Promise[Unit]] = booksSubspace.watch(key)
val watch: Promise[Unit] = dbio.transact(database)
```   

More information about watches can be found in 
[FoundationDB developer guide](https://apple.github.io/foundationdb/developer-guide.html#watches) and
[FoundationDB Javadoc](https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/Transaction.html#watch-byte:A-).  

## Reading large amount of data
If you want to stream data from a subspace, it can take longer than FoundationDb transaction time limit,
and your data is immutable and append only, or if approximation is good enough for your use case, 
you can use either use `SubspaceSource` (from akka-streams module) 
or you can use `RefreshingSubspaceStream` (from core module).

Advantage of using `SubspaceSource` is that it closes resources automatically and 
exposes easier to use API leveraging Akka Streams.

To create a source you need at least `subspace: TypedSubspace[Entity, Key]` and `database: Database`: 
```scala
val source: Source[Entity, _] = SubspaceSource.from(subspace, database)
```

However, if you don't want to add Akka as a dependency or you need more control over streaming the data 
you can use `RefreshingSubspaceStream`.

### Processing data continuously
If a subspace (or part of a subspace) is modeled as a log and one wants to process the data once it arrives, 
`InfinitePollingSubspaceSource` may become useful.

`InfinitePollingSubspaceSource` will stream the data from the subspace (or from part of a subspace). 
Once it reaches the last element it will try to resume from the last seen value.

```scala
val processEntityFlow: Flow[Entity, Entity, NotUsed] = ???
val commitOffsetSink: Sink[Entity, NotUsed] = ???
val lastSeenKey: Array[Byte] = fetchLastSeenKey()
val source = Source[Entity, _] = 
  InfinitePollingSubspaceSource.from(
    typedSubspace, 
    database, 
    pollingInterval = 100.millis, 
    begin = KeySelector.firstGreaterThan(lastSeenKey))
source.via(processEntityFlow).to(commitOffsetSink).run()
```

## Example - class scheduling
Module `example` contains implementation of [Class Scheduling from FoundationDB website](https://apple.github.io/foundationdb/class-scheduling-java.html).

## Contributing
Contributors and help is always welcome!

Please make sure that issue exists for the functionality that you want to create (or bug that you want to fix),
and in the commit message please include issue number and issue title (e.g. "#1 Incorrect ...").

## Testing
To run tests you'll need a local FoundationDB instance running (here are installation instructions for [Linux](https://apple.github.io/foundationdb/getting-started-linux.html) and [macOS](https://apple.github.io/foundationdb/getting-started-mac.html)).

Then simply execute `sbt test`.
