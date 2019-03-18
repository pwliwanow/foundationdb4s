import sbt._
import scala.collection.immutable.Seq

object Dependencies {

  private val akkaVersion = "2.5.21"
  private lazy val akkaStreams = "com.typesafe.akka" %% "akka-stream" % akkaVersion
  private lazy val akkaStreamsTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion

  private val catsVersion = "1.6.0"
  private lazy val cats = "org.typelevel" %% "cats-core" % catsVersion
  private lazy val catsLaws = "org.typelevel" %% "cats-laws" % catsVersion

  private val foundationDbVersion = "6.0.15"
  private lazy val foundationDb = "org.foundationdb" % "fdb-java" % foundationDbVersion withSources()

  private val java8CompatVersion = "0.9.0"
  private lazy val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % java8CompatVersion

  private val mockitoVersion = "2.25.1"
  private lazy val mockito = "org.mockito" % "mockito-core" % mockitoVersion

  private lazy val akkaStreamsDependencies: Seq[ModuleID] = Seq(akkaStreams)
  private lazy val coreDependencies: Seq[ModuleID] = Seq(cats, foundationDb, java8Compat)

  private val scalaTestVersion = "3.0.6"
  private lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

  private lazy val akkaStreamsTestDependencies: Seq[ModuleID] =
    Seq(akkaStreamsTestKit).map(_ % Test)
  private lazy val coreTestDependencies: Seq[ModuleID] =
    Seq(catsLaws, mockito, scalaTest).map(_ % Test)

  lazy val allAkkaStreamsDependencies
    : Seq[ModuleID] = akkaStreamsDependencies ++ akkaStreamsTestDependencies
  lazy val allCoreDependencies: Seq[ModuleID] = coreDependencies ++ coreTestDependencies
}
