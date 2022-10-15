import Dependencies._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.ReleasePlugin.autoImport._

lazy val scala2_13 = "2.13.10"
lazy val supportedScalaVersions = List(scala2_13)

ThisBuild / scalaVersion := scala2_13

lazy val fdb4s = project
  .in(file("."))
  .settings(commonSettings: _*)
  .settings(publish / skip := true, crossScalaVersions := Nil)
  .aggregate(akkaStreams, core, example, schema)

lazy val core = project
  .in(file("core"))
  .settings(
    commonSettings,
    name := "core",
    libraryDependencies ++= allCoreDependencies,
    crossScalaVersions := supportedScalaVersions
  )

lazy val schema = project
  .in(file("schema"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    commonSettings,
    name := "schema",
    libraryDependencies ++= allSchemaDependencies,
    crossScalaVersions := supportedScalaVersions
  )

lazy val akkaStreams = project
  .in(file("akka-streams"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    commonSettings,
    name := "akka-streams",
    libraryDependencies ++= allAkkaStreamsDependencies,
    crossScalaVersions := supportedScalaVersions
  )

lazy val example = project
  .in(file("example"))
  .dependsOn(schema)
  .settings(
    commonSettings,
    name := "example",
    publish / skip := true,
    crossScalaVersions := supportedScalaVersions,
    coverageEnabled := false
  )

lazy val commonSettings = ossPublishSettings ++ Seq(
  organization := "com.github.pwliwanow.foundationdb4s",
  scalaVersion := scala2_13,
  scalafmtOnCompile := true,
  parallelExecution := false,
  fork := true,
  scalacOptions ++= {
    List(
      "-unchecked",
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-feature",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-Xlint:inaccessible",
      "-Xlint:infer-any",
      "-Ywarn-dead-code",
      "-Ywarn-unused:imports",
      "-Ywarn-unused:locals",
      "-Ywarn-unused:patvars",
      "-Ywarn-unused:privates",
      "-Xfatal-warnings"
    )
  },
  Compile / doc / scalacOptions ++= Seq(
    "-no-link-warnings"
  )
)

lazy val ossPublishSettings = Seq(
  publishTo := {
    if (isSnapshot.value) Some(Opts.resolver.sonatypeSnapshots)
    else sonatypePublishToBundle.value
  },
  Test / publishArtifact := false,
  publishMavenStyle := true,
  sonatypeProfileName := "com.github.pwliwanow",
  pomIncludeRepository := { _ =>
    false
  },
  credentials += Credentials(Path.userHome / ".sbt" / ".credentials"),
  organizationHomepage := Some(url("https://github.com/pwliwanow/foundationdb4s")),
  homepage := Some(url("https://github.com/pwliwanow/foundationdb4s")),
  licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/pwliwanow/foundationdb4s"),
      "scm:git:https://github.com/pwliwanow/foundationdb4s.git"
    )
  ),
  autoAPIMappings := true,
  developers := List(
    Developer(
      id = "pwliwanow",
      name = "Pawel Iwanow",
      email = "pwliwanow@gmail.com",
      url = new URL("https://github.com/pwliwanow/")
    )
  ),
  // sbt-release
  releaseCrossBuild := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseIgnoreUntrackedFiles := true,
  releaseProcess := Seq(
    checkSnapshotDependencies,
    inquireVersions,
    // publishing locally so that the pgp password prompt is displayed early
    // in the process
    releaseStepCommandAndRemaining("+publishLocalSigned"),
    releaseStepCommandAndRemaining("+clean"),
    releaseStepCommandAndRemaining("+test"),
    setReleaseVersion,
    releaseStepTask(updateVersionInReadme),
    commitReleaseVersion,
    tagRelease,
    releaseStepCommandAndRemaining("+publishSigned"),
    releaseStepCommand("sonatypeBundleRelease"),
    setNextVersion,
    commitNextVersion,
    pushChanges
  )
)

lazy val updateVersionInReadme =
  taskKey[Unit]("Updates version in README.md to the one present in version.sbt")

updateVersionInReadme := {
  import java.io.PrintWriter
  import scala.io.Source
  val pattern = """val fdb4sVersion = "([^\"]*)""""
  val source = Source.fromFile("README.md")
  val updatedReadme = source
    .getLines()
    .map { line =>
      if (line.matches(pattern)) s"""val fdb4sVersion = "${version.value}""""
      else line
    }
    .mkString("\n") + "\n"
  source.close()
  new PrintWriter("README.md") { write(updatedReadme); close() }
}
