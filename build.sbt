organization := "com.jamesward"

name := "zio-mavencentral"

scalaVersion := "3.8.3"

scalacOptions ++= Seq(
  // "-Yexplicit-nulls", // not sure where it went
  "-language:strictEquality",
  "-deprecation",
  // "-Xfatal-warnings", // not sure where it went
)

val zioVersion = "2.1.26"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"                 % zioVersion,
  "dev.zio" %% "zio-concurrent"      % zioVersion,
  "dev.zio" %% "zio-direct"          % "1.0.0-RC7",
  "dev.zio" %% "zio-http"            % "3.11.1",
  "dev.zio" %% "zio-schema-derivation" % "1.8.5",

  "nl.vroste" %% "rezilience" % "0.10.5",

  "org.bouncycastle" % "bcpg-jdk18on" % "1.84",

  "org.scala-lang.modules" %% "scala-xml" % "2.4.0",

  "de.sciss" %% "semverfi" % "0.3.0",

  "dev.zio" %% "zio-test"           % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt"       % zioVersion % Test,
  "dev.zio" %% "zio-test-magnolia"  % zioVersion % Test,
)

fork := true

javaOptions ++= Seq(
  "-Djava.net.preferIPv4Stack=true",
  // JDK 25: suppress sun.misc.Unsafe / restricted-method warnings
  // emitted by upstream libs (scala-library, netty-common).
  "--enable-native-access=ALL-UNNAMED",
  "--sun-misc-unsafe-memory-access=allow",
)

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
