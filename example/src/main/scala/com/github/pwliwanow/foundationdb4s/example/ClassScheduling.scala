package com.github.pwliwanow.foundationdb4s.example

import java.time.LocalTime

import cats.instances.list._
import cats.syntax.traverse._
import com.apple.foundationdb.{Database, FDB}
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO}
import com.github.pwliwanow.foundationdb4s.example.Model.{Attendance, Class}
import com.github.pwliwanow.foundationdb4s.schema.{Schema, TupleDecoder, TupleEncoder}
import shapeless.{::, HNil}

import scala.collection.immutable.Seq
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Random

object Model {
  object ClassLevel extends Enumeration {
    type ClassLevel = Value
    val Intro, ForDummies, Remedial, `101`, `201`, `301`, Mastery, Lab, Seminar = Value
  }
  object ClassType extends Enumeration {
    type ClassType = Value
    val Chem, Bio, CS, Geometry, Calc, Alg, Film, Music, Art, Dance = Value
  }
  import ClassLevel._
  import ClassType._

  final case class Class(time: LocalTime, `type`: ClassType, level: ClassLevel)
  final case class StudentId(value: String) extends AnyVal
  final case class Attendance(studentId: StudentId, `class`: Class)
  final case class ClassAvailability(`class`: Class, seatsAvailable: Int)
}
import Model._
import ClassLevel._
import ClassType._

object Codecs {
  implicit lazy val classLevelDec = implicitly[TupleDecoder[String]].map(ClassLevel.withName)
  implicit lazy val classLevelEnc =
    implicitly[TupleEncoder[String]].contramap[ClassLevel](_.toString)

  implicit lazy val classTypeDec = implicitly[TupleDecoder[String]].map(ClassType.withName)
  implicit lazy val classTypeEnc =
    implicitly[TupleEncoder[String]].contramap[ClassType](_.toString)

  implicit lazy val localTimeDec = implicitly[TupleDecoder[Long]].map(LocalTime.ofSecondOfDay)
  implicit lazy val localTimeEnc =
    implicitly[TupleEncoder[Long]].contramap[LocalTime](_.toSecondOfDay.toLong)

  implicit lazy val classDec = TupleDecoder.derive[Class]
  implicit lazy val classEnc = TupleEncoder.derive[Class]

  implicit lazy val attendanceDec = TupleDecoder.derive[Attendance]
  implicit lazy val attendanceEnc = TupleEncoder.derive[Attendance]

  implicit lazy val classAvailabilityDec = TupleDecoder.derive[ClassAvailability]
  implicit lazy val classAvailabilityEnc = TupleEncoder.derive[ClassAvailability]
}
import Codecs._

object Dao {
  object AttendanceSchema extends Schema {
    type Entity = Attendance
    type KeySchema = StudentId :: Class :: HNil
    type ValueSchema = HNil
    override def toKey(entity: Attendance): KeySchema =
      entity.studentId :: entity.`class` :: HNil
    override def toValue(entity: Attendance): ValueSchema =
      HNil
    override def toEntity(key: KeySchema, valueRepr: HNil): Attendance = {
      val student :: c :: HNil = key
      Attendance(student, c)
    }
  }

  object ClassAvailabilitySchema extends Schema {
    type Entity = ClassAvailability
    type KeySchema = LocalTime :: ClassType :: ClassLevel :: HNil
    type ValueSchema = Int :: HNil
    override def toKey(entity: ClassAvailability): KeySchema =
      entity.`class`.time :: entity.`class`.`type` :: entity.`class`.level :: HNil
    override def toValue(entity: ClassAvailability): ValueSchema =
      entity.seatsAvailable :: HNil
    override def toEntity(key: KeySchema, valueRepr: ValueSchema): ClassAvailability = {
      val classTime :: classType :: classLevel :: HNil = key
      val seatsAvailable :: HNil = valueRepr
      ClassAvailability(Class(classTime, classType, classLevel), seatsAvailable)
    }
  }
}
import Dao._

object ClassScheduling {
  implicit val executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  val database: Database = FDB.selectAPIVersion(620).open(null, executor)

  val schedulingSubspace = new Subspace(Tuple.from("class-scheduling"))

  val attendanceNamespace: AttendanceSchema.Namespace = {
    val subspace = schedulingSubspace.subspace(Tuple.from("attendance"))
    new AttendanceSchema.Namespace(subspace)
  }
  val classAvailabilityNamespace: ClassAvailabilitySchema.Namespace = {
    val subspace = schedulingSubspace.subspace(Tuple.from("class"))
    new ClassAvailabilitySchema.Namespace(subspace)
  }

  val classes: List[Class] = for {
    level <- ClassLevel.values.toList
    tpe <- ClassType.values.toList
    time <- (2 to 19).map(h => LocalTime.of(h, 0))
  } yield Class(time = time, `type` = tpe, level = level)

  val classAvailabilities: List[ClassAvailability] =
    classes.map(c => ClassAvailability(`class` = c, seatsAvailable = 100))

  def init(): Future[Unit] = {
    val dbio: DBIO[Unit] = for {
      _ <- attendanceNamespace.clear()
      _ <- classAvailabilityNamespace.clear()
      _ <- classAvailabilities.map(classAvailabilityNamespace.set).sequence
    } yield ()
    dbio.transact(database)
  }

  def availableClasses: ReadDBIO[Seq[ClassAvailability]] = {
    classAvailabilityNamespace
      .getRange(classAvailabilityNamespace.range())
      .map(classes => classes.filter(_.seatsAvailable > 0))
  }

  def signup(studentId: StudentId, c: Class): DBIO[Unit] = {
    def enroll: DBIO[Unit] =
      for {
        ca <- getClassAvailability(c)
        _ <- ca.checkThat(_.seatsAvailable > 0)("No remaining seats")
        attendances <- attendanceNamespace.getRange(Tuple1(studentId)).toDBIO
        _ <- attendances.checkThat(_.size < 5)("Too many classes")
        _ <- classAvailabilityNamespace.set(ca.copy(seatsAvailable = ca.seatsAvailable - 1))
        _ <- attendanceNamespace.set(Attendance(studentId, ca.`class`))
      } yield ()
    for {
      maybeAttendance <- attendanceNamespace.getRow((studentId, c)).toDBIO
      _ <-
        maybeAttendance
          .map(_ => DBIO.unit) // already signed up
          .getOrElse(enroll)
    } yield ()
  }

  def drop(studentId: StudentId, c: Class): DBIO[Unit] = {
    def doDropClass(a: Attendance): DBIO[Unit] =
      for {
        ca <- getClassAvailability(a.`class`)
        _ <- classAvailabilityNamespace.set(ca.copy(seatsAvailable = ca.seatsAvailable - 1))
        _ <- attendanceNamespace.clear()
      } yield ()
    for {
      maybeAttendance <- attendanceNamespace.getRow((studentId, c)).toDBIO
      _ <- maybeAttendance.fold[DBIO[Unit]](DBIO.unit)(doDropClass)
    } yield ()
  }

  def switchClasses(studentId: StudentId, oldC: Class, newC: Class): DBIO[Unit] =
    for {
      _ <- drop(studentId, oldC)
      _ <- signup(studentId, newC)
    } yield ()

  private def getClassAvailability(c: Class): DBIO[ClassAvailability] =
    for {
      maybeClass <- classAvailabilityNamespace.getRow(c).toDBIO
      _ <- maybeClass.checkThat(_.isDefined)("Class does not exist")
    } yield maybeClass.get

  implicit class ValueHolder[A](value: A) {
    def checkThat(f: A => Boolean)(errorMsg: String): DBIO[Unit] =
      if (f(value)) DBIO.unit
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
    database.close()
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
    val studentId = StudentId(s"s${i.toString}")
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
        _ <-
          if (allClasses.isEmpty)
            availableClasses.transact(database).map(xs => allClasses = xs)
          else Future.successful(())
        _ <- mood match {
          case Add =>
            val c = allClasses(rand.nextInt(allClasses.size))
            signup(studentId, c.`class`)
              .transact(database)
              .map(_ => myClasses += c)
          case Drop =>
            val index = rand.nextInt(myClasses.size)
            val c = myClasses(index)
            drop(studentId, c.`class`).transact(database).map(_ => myClasses.remove(index))
          case Switch =>
            val oldCIndex = rand.nextInt(myClasses.size)
            val newCIndex = rand.nextInt(allClasses.size)
            val oldC = myClasses(oldCIndex)
            val newC = allClasses(newCIndex)
            switchClasses(studentId, oldC.`class`, newC.`class`)
              .transact(database)
              .map { _ =>
                myClasses.remove(oldCIndex)
                myClasses += newC
              }
        }
      } yield ()
      f.recover { case e =>
        println(e.getMessage + " Need to recheck available classes.")
        allClasses = Seq.empty
      }
    }
    (1 to ops).foldLeft[Future[Unit]](Future.successful(())) { (acc, _) =>
      acc.flatMap(_ => perfromSimulationIteration())
    }
  }
}
