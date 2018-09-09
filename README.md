# foundationdb4s

foundationdb4s is a wrapper for [FoundationDb](https://github.com/apple/foundationdb) Java client.
It aims to be type-safe and idiomatic for Scala:

```scala
implicit val ec = scala.concurrent.ExecutionContext.global
val transactor = Transactor(version = 520)

final case class Book(isbn: String, title: String, publishedOn: LocalDate)
val userSubspace = new TypedSubspace[Book, String] {
  override val subspace: Subspace = new Subspace(Tuple.from("books"))
  override def toKey(entity: Book): String = entity.isbn
  override def toRawValue(entity: Book): Array[Byte] = {
    Tuple.from(entity.title, entity.publishedOn.toString).pack
  }
  override protected def toTupledKey(key: String): Tuple = Tuple.from(key)
  override protected def toKey(tupledKey: Tuple): String = tupledKey.getString(0)
  override protected def toEntity(key: String, value: Array[Byte]): Book = {
    val tupledValue = Tuple.fromBytes(value)
    val publishedOn = LocalDate.parse(tupledValue.getString(1))
    Book(isbn = key, title = tupledValue.getString(0), publishedOn = publishedOn)
  }
}
val dbio: DBIO[Option[Book]] = for {
  _ <- userSubspace.set(Book("978-0451205766", "The Godfather", LocalDate.parse("2002-03-01")))
  maybeBook <- userSubspace.get("978-0451205766")
} yield maybeBook
val maybeBook: Future[Option[Book]] = dbio.transact(transactor)
```

The library is still incomplete and does not cover everything official client does.

If you:
- think some functionality is missing
- find a bug 
- feel that API does not "feel right"

please create an issue.

### Basic abstractions
Reading from a `TypedSubspace` (`get` and `getRange` operations) returns `ReadDBIO[_]` monad.

Modifying data within a `TypedSubspace` (`clear` and `set` operations) returns `DBIO[_]` monad.

Having `read: ReadDBIO[Unit]` and `set: DBIO[Unit]`, 
both `read.flatMap(_ => set)` and `set.flatMap(_ => read)` will result in `DBIO[Unit]`.

### Integrations
- Cats - Monad instances are provided in companion objects for `DBIO` and `ReadDBIO`
- Akka Streams `Source` implementation (`akka-streams` module)

### Akka streams
If you want to stream data from a subspace, it can take longer than FoundationDb transaction time limit,
and your data is immutable and append only, or if approximation is good enough for your use case, 
you can use `SubspaceSource`. 

To create a source you need at least `subspace: TypedSubspace[Entity, Key]` and `transactor: Transactor`: 
```scala
val source: Source[Entity] = SupspaceSource.from(subspace, transactor)
```
### Example - class scheduling
Module `example` contains implementation of [Class Scheduling from FoundationDB website]((https://apple.github.io/foundationdb/class-scheduling-java.html)).

### Contributing
Contributors and help is always welcome!

Please make sure that issue exists for the functionality that you want to create (or bug that you want to fix),
and in the commit message please include issue number and issue title (e.g. "#1 Incorrect ...").

### Testing
To run tests you'll need a local FoundationDB instance running (here are installation instructions for [Linux](https://apple.github.io/foundationdb/getting-started-linux.html) and [macOS](https://apple.github.io/foundationdb/getting-started-mac.html)).

Then simply execute `sbt test`. 
