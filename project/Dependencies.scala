import sbt._
import scala.collection.immutable.Seq

object Dependencies {

  val akkaVersion = "2.5.16"
  lazy val akkaStreams = "com.typesafe.akka" %% "akka-stream" % akkaVersion
  lazy val akkaStreamsTestKit = "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion

  val catsVersion = "1.3.1"
  lazy val cats = "org.typelevel" %% "cats-core" % catsVersion
  lazy val catsLaws = "org.typelevel" %% "cats-laws" % catsVersion

  val foundationDbVersion = "5.2.5"
  lazy val foundationDb = "org.foundationdb" % "fdb-java" % foundationDbVersion

  val java8CompatVersion = "0.9.0"
  lazy val java8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % java8CompatVersion

  val scalaMockVersion = "4.1.0"
  lazy val scalaMock = "org.scalamock" %% "scalamock" % scalaMockVersion

  lazy val akkaStreamsDependencies: Seq[ModuleID] = Seq(akkaStreams)
  lazy val coreDependencies: Seq[ModuleID] = Seq(cats, foundationDb, java8Compat)
  lazy val exampleDependencies: Seq[ModuleID] = Seq.empty

  val scalaTestVersion = "3.0.5"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

  lazy val akkaStreamsTestDependencies: Seq[ModuleID] =
    Seq(akkaStreamsTestKit, scalaMock).map(_ % Test)
  lazy val coreTestDependencies: Seq[ModuleID] = Seq(catsLaws, scalaTest).map(_ % Test)
  lazy val exampleTestDependencies: Seq[ModuleID] = Seq.empty[ModuleID]

  lazy val allAkkaStreamsDependencies
    : Seq[ModuleID] = akkaStreamsDependencies ++ akkaStreamsTestDependencies
  lazy val allCoreDependencies: Seq[ModuleID] = coreDependencies ++ coreTestDependencies
  lazy val allExampleDependencies: Seq[ModuleID] = coreDependencies ++ coreTestDependencies
}
