organization := "com.jamesward"

name := "zio-mavencentral"

scalaVersion := "3.8.1"

scalacOptions ++= Seq(
  // "-Yexplicit-nulls", // not sure where it went
  "-language:strictEquality",
  // "-Xfatal-warnings", // not sure where it went
)

val zioVersion = "2.1.24"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"                 % zioVersion,
  "dev.zio" %% "zio-direct"          % "1.0.0-RC7",
  "dev.zio" %% "zio-direct-streams"  % "1.0.0-RC7",
  "dev.zio" %% "zio-http"            % "3.8.0",
  "dev.zio" %% "zio-streams-compress-zip" % "1.1.3",
  "dev.zio" %% "zio-schema-derivation" % "1.7.6",

  "org.scala-lang.modules" %% "scala-xml" % "2.4.0",

  "de.sciss" %% "semverfi" % "0.3.0",

  "dev.zio" %% "zio-test"           % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt"       % zioVersion % Test,
  "dev.zio" %% "zio-test-magnolia"  % zioVersion % Test,
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

fork := true

javaOptions += "-Djava.net.preferIPv4Stack=true"

licenses := Seq("MIT License" -> url("https://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/jamesward/zio-mavencentral"))

developers := List(
  Developer(
    "jamesward",
    "James Ward",
    "james@jamesward.com",
    url("https://jamesward.com")
  )
)

ThisBuild / versionScheme := Some("semver-spec")
