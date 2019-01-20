package com.github.pwliwanow.foundationdb4s.example

import java.nio.ByteBuffer

import cats.instances.list._
import cats.syntax.traverse._
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO, Transactor, TypedSubspace}

import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Random

object ClassScheduling {
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  val transactor = Transactor(version = 600)

  final case class Attendance(student: String, `class`: Class)
  final case class ClassAvailability(`class`: Class, seatsAvailable: Int)
  final case class Class(time: String, `type`: String, level: String)

  val attendanceSubspace: TypedSubspace[Attendance, Attendance] =
    new TypedSubspace[Attendance, Attendance] {
      override val subspace: com.apple.foundationdb.subspace.Subspace =
        transactor.createOrOpen("class-scheduling", "attendence")
      override def toKey(entity: Attendance): Attendance = entity
      override def toTupledKey(key: Attendance): Tuple = {
        Tuple.from(key.student, key.`class`.time, key.`class`.`type`, key.`class`.level)
      }
      override def toRawValue(entity: Attendance): Array[Byte] = Array.emptyByteArray
      override def toKey(tupledKey: Tuple): Attendance = {
        val c = Class(
          time = tupledKey.getString(1),
          `type` = tupledKey.getString(2),
          level = tupledKey.getString(3))
        Attendance(student = tupledKey.getString(0), `class` = c)
      }

      override protected def toEntity(key: Attendance, value: Array[Byte]): Attendance = key
    }

  val classAvailabilitySubspace: TypedSubspace[ClassAvailability, Class] =
    new TypedSubspace[ClassAvailability, Class] {
      override val subspace: com.apple.foundationdb.subspace.Subspace =
        transactor.createOrOpen("class-scheduling", "class")
      override def toKey(entity: ClassAvailability): Class = entity.`class`
      override def toTupledKey(key: Class): Tuple =
        Tuple.from(key.time, key.`type`, key.level)
      override def toRawValue(entity: ClassAvailability): Array[Byte] =
        encodeInt(entity.seatsAvailable)
      override def toKey(tupledKey: Tuple): Class =
        Class(
          time = tupledKey.getString(0),
          `type` = tupledKey.getString(1),
          level = tupledKey.getString(2))
      override def toEntity(key: Class, value: Array[Byte]): ClassAvailability =
        ClassAvailability(`class` = key, seatsAvailable = decodeInt(value))
    }

  val levels =
    List("intro", "for dummies", "remedial", "101", "201", "301", "mastery", "lab", "seminar")
  val types = List("chem", "bio", "cs", "geometry", "calc", "alg", "film", "music", "art", "dance")
  val times: Seq[String] = (2 to 19).map(h => s"$h:00")

  val classes: List[Class] = for {
    level <- levels
    tpe <- types
    time <- times
  } yield Class(time = time, `type` = tpe, level = level)

  val classAvailabilities: List[ClassAvailability] =
    classes.map(c => ClassAvailability(`class` = c, seatsAvailable = 100))

  def init(): Future[Unit] = {
    val dbio: DBIO[Unit] = for {
      _ <- attendanceSubspace.clear()
      _ <- classAvailabilitySubspace.clear()
      _ <- classAvailabilities.map(classAvailabilitySubspace.set).sequence
    } yield ()
    dbio.transact(transactor)
  }

  def availableClasses: ReadDBIO[Seq[ClassAvailability]] = {
    classAvailabilitySubspace
      .getRange(classAvailabilitySubspace.range())
      .map(classes => classes.filter(_.seatsAvailable > 0))
  }

  def signup(s: String, c: Class): DBIO[Unit] = {
    def enroll: DBIO[Unit] =
      for {
        ca <- getClassAvailability(c)
        _ <- ca.checkThat(_.seatsAvailable > 0)("No remaining seats")
        attendances <- attendanceSubspace.getRange(attendanceSubspace.range(Tuple.from(s)))
        _ <- attendances.checkThat(_.size < 5)("Too many classes")
        _ <- classAvailabilitySubspace.set(ca.copy(seatsAvailable = ca.seatsAvailable - 1))
        _ <- attendanceSubspace.set(Attendance(s, ca.`class`))
      } yield ()
    for {
      maybeAttendance <- attendanceSubspace.get(Attendance(s, c))
      _ <- maybeAttendance
        .map(_ => DBIO.pure[Unit](())) // already signed up
        .getOrElse(enroll)
    } yield ()
  }

  def drop(s: String, c: Class): DBIO[Unit] = {
    def doDropClass(a: Attendance): DBIO[Unit] =
      for {
        ca <- getClassAvailability(a.`class`)
        _ <- classAvailabilitySubspace.set(ca.copy(seatsAvailable = ca.seatsAvailable - 1))
        _ <- attendanceSubspace.clear()
      } yield ()
    for {
      maybeAttendence <- attendanceSubspace.get(Attendance(s, c))
      _ <- maybeAttendence.fold[DBIO[Unit]](DBIO.pure(()))(doDropClass)
    } yield ()
  }

  def switchClasses(s: String, oldC: Class, newC: Class): DBIO[Unit] =
    for {
      _ <- drop(s, oldC)
      _ <- signup(s, newC)
    } yield ()

  private def getClassAvailability(c: Class): DBIO[ClassAvailability] =
    for {
      maybeClass <- classAvailabilitySubspace.get(c)
      _ <- maybeClass.checkThat(_.isDefined)("Class does not exist")
    } yield maybeClass.get

  private def encodeInt(value: Int): Array[Byte] = {
    val output = new Array[Byte](4)
    ByteBuffer.wrap(output).putInt(value)
    output
  }

  private def decodeInt(value: Array[Byte]): Int = {
    if (value.length != 4) throw new IllegalArgumentException("Array must be of size 4")
    ByteBuffer.wrap(value).getInt
  }

  implicit class ValueHolder[A](value: A) {
    def checkThat(f: A => Boolean)(errorMsg: String): DBIO[Unit] =
      if (f(value)) DBIO.pure(())
      else DBIO.failed(new IllegalStateException(errorMsg))
  }

  //
  // Testing
  //

  def main(args: Array[String]): Unit = {
    val (students, opsPerStudent) = (100, 50)
    val (_, timeInMs) = measure {
      val future = for {
        _ <- init()
        _ <- runSim(students, opsPerStudent)
      } yield ()
      Await.result(future, Duration.Inf)
    }
    val numberOfTxs = students * opsPerStudent
    val txPerSec = numberOfTxs / (timeInMs / 1000)
    println(
      s"Ran $numberOfTxs transactions in ${timeInMs}ms. Throughput: $txPerSec transactions per second.")
    transactor.close()
  }

  private def measure[A](f: => A): (A, Long) = {
    val start = System.nanoTime()
    val res = f
    val end = System.nanoTime()
    val diffMs = (end - start) / 10e5.toLong
    (res, diffMs)
  }

  private def runSim(students: Int, opsPerStudent: Int): Future[Unit] = {
    val fs = (0 until students).toList.map(i => simulateStudents(i, opsPerStudent))
    Future.sequence(fs).map(_ => ())
  }

  sealed trait Operation
  final case object Add extends Operation
  final case object Switch extends Operation
  final case object Drop extends Operation

  private def simulateStudents(i: Int, ops: Int): Future[Unit] = {
    val studentID = i.toString
    var allClasses: Seq[ClassAvailability] = classAvailabilities
    val myClasses = ArrayBuffer.empty[ClassAvailability]
    val rand = new Random()

    def perfromSimulationIteration(): Future[Unit] = {
      val classCount = myClasses.size
      val moods = ArrayBuffer.empty[Operation]
      if (classCount > 1) {
        moods += Drop
        moods += Switch
      }
      if (classCount < 5) moods += Add
      val mood = moods(rand.nextInt(moods.size))
      val f: Future[Unit] = for {
        _ <- if (allClasses.isEmpty)
          availableClasses.transact(transactor).map(xs => allClasses = xs)
        else Future.successful(())
        _ <- mood match {
          case Add =>
            val c = allClasses(rand.nextInt(allClasses.size))
            signup(studentID, c.`class`)
              .transact(transactor)
              .map(_ => myClasses += c)
          case Drop =>
            val index = rand.nextInt(myClasses.size)
            val c = myClasses(index)
            drop(studentID, c.`class`).transact(transactor).map(_ => myClasses.remove(index))
          case Switch =>
            val oldCIndex = rand.nextInt(myClasses.size)
            val newCIndex = rand.nextInt(allClasses.size)
            val oldC = myClasses(oldCIndex)
            val newC = allClasses(newCIndex)
            switchClasses(studentID, oldC.`class`, newC.`class`)
              .transact(transactor)
              .map { _ =>
                myClasses.remove(oldCIndex)
                myClasses += newC
              }
        }
      } yield ()
      f.recover {
        case e =>
          println(e.getMessage + " Need to recheck available classes.")
          allClasses = Seq.empty
      }
    }
    (1 to ops).foldLeft[Future[Unit]](Future.successful(())) { (acc, _) =>
      acc.flatMap(_ => perfromSimulationIteration())
    }
  }
}
