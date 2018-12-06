import Dependencies._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.ReleasePlugin.autoImport._

lazy val fdb4s = project
  .in(file("."))
  .settings(commonSettings: _*)
  .settings(skip in publish := true)
  .aggregate(akkaStreams, core, example)

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
    skip in publish := true
  )

lazy val commonSettings = buildSettings ++ Seq(
  organization := "com.github.pwliwanow.foundationdb4s",
  scalaVersion := "2.12.8",
  scalafmtOnCompile := true,
  coverageExcludedPackages := "com.github.pwliwanow.foundationdb4s.example.*",
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

lazy val buildSettings =
  commonSmlBuildSettings ++
    acyclicSettings ++
    ossPublishSettings

lazy val ossPublishSettings = Seq(
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  ),
  publishArtifact in Test := false,
  publishMavenStyle := true,
  sonatypeProfileName := "com.github.pwliwanow.foundationdb4s",
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
  releaseProcess := Release.steps(organization.value)
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
