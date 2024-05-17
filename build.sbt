organization := "com.jamesward"

name := "zio-mavencentral"

scalaVersion := "3.4.1"

scalacOptions ++= Seq(
  "-Yexplicit-nulls",
  "-language:strictEquality",
  "-Xfatal-warnings",
)

val zioVersion = "2.1.1"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"                 % zioVersion,
  "dev.zio" %% "zio-direct"          % "1.0.0-RC7",
  "dev.zio" %% "zio-direct-streams"  % "1.0.0-RC7",
  "dev.zio" %% "zio-http"            % "3.0.0-RC7",

  "org.apache.commons" %  "commons-compress" % "1.26.1",

  "de.sciss" %% "semverfi" % "0.3.0",

  "dev.zio" %% "zio-test"           % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt"       % zioVersion % Test,
  "dev.zio" %% "zio-test-magnolia"  % zioVersion % Test,
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

fork := true

javaOptions += "-Djava.net.preferIPv4Stack=true"

licenses := Seq("MIT License" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/jamesward/zio-mavencentral"))

developers := List(
  Developer(
    "jamesward",
    "James Ward",
    "james@jamesward.com",
    url("https://jamesward.com")
  )
)
