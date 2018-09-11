import Dependencies._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.ReleasePlugin.autoImport._

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .settings(skip in publish := true)
  .aggregate(akkaStreams, core)

lazy val core = project
  .in(file("core"))
  .settings(
    commonSettings,
    name := "foundationdb4s-core",
    libraryDependencies ++= allCoreDependencies
  )

lazy val akkaStreams = project
  .in(file("akka-streams"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    commonSettings,
    name := "foundationdb4s-akka-streams",
    libraryDependencies ++= allAkkaStreamsDependencies
  )

lazy val example = project
  .in(file("example"))
  .dependsOn(core)
  .settings(
    commonSettings,
    name := "foundationdb4s-example",
    libraryDependencies ++= allExampleDependencies
  )

lazy val commonSettings = smlBuildSettings ++ Seq(
  organization := "com.github.pwliwanow.foundationdb4s",
  scalaVersion := "2.12.6",
  scalafmtOnCompile := true,
  releaseProcess := Seq(
    checkSnapshotDependencies,
    inquireVersions,
    // publishing locally so that the pgp password prompt is displayed early
    // in the process
    releaseStepCommandAndRemaining("+publishLocalSigned"),
    releaseStepCommandAndRemaining("+clean"),
    releaseStepCommandAndRemaining("+test"),
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishSigned"),
    setNextVersion,
    commitNextVersion,
    releaseStepCommand("sonatypeReleaseAll"),
    pushChanges
  ),
  parallelExecution in ThisBuild := false,
  fork := true,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-explaintypes",
    "-feature",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-Xfatal-warnings",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
    "-Ywarn-dead-code",
    "-Ypartial-unification",
    "-Ywarn-unused:implicits",
    "-Ywarn-unused:imports",
    "-Ywarn-unused:locals",
    "-Ywarn-unused:params",
    "-Ywarn-unused:patvars",
    "-Ywarn-unused:privates"
  ),
  scalacOptions in (Compile, doc) ++= Seq(
    "-no-link-warnings"
  )
)

lazy val smlBuildSettings =
  commonSmlBuildSettings ++
    acyclicSettings ++
    ossPublishSettings
