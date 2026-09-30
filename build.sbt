organization := "com.jamesward"

name := "zio-mavencentral"

scalaVersion := "3.9.0"

scalacOptions ++= Seq(
  // "-Yexplicit-nulls", // not sure where it went
  "-language:strictEquality",
  "-deprecation",
  "-Werror",
)

// sbt-mcp settings (loopback-only: its tools can execute build tasks)
mcpEnabled := true
mcpHost := "127.0.0.1"
mcpPort := 5121

// SkillsJars: extract agent Skills with `./sbt extractSkillsJars`
skillsJarsOutputDir := Some(file(".kiro/skills"))

val zioVersion = "2.1.26"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio"                 % zioVersion,
  "dev.zio" %% "zio-concurrent"      % zioVersion,
  "dev.zio" %% "zio-direct"          % "1.0.0-RC7", // no stable zio-direct release exists yet
  "dev.zio" %% "zio-http"            % "3.11.6",
  "dev.zio" %% "zio-schema-derivation" % "1.9.0",

  "nl.vroste" %% "rezilience" % "0.10.5",

  "org.bouncycastle" % "bcpg-jdk18on" % "1.86",

  "org.scala-lang.modules" %% "scala-xml" % "2.5.0",

  // coursier's version ordering (Maven/Aether-compatible) and pre-release qualifiers
  "io.get-coursier" %% "versions" % "0.6.1",

  "dev.zio" %% "zio-test"           % zioVersion % Test,
  "dev.zio" %% "zio-test-sbt"       % zioVersion % Test,

  "com.jamesward" % "skills" % "0.0.4" % Skills,
)

fork := true

javaOptions ++= Seq(
  "-Djava.net.preferIPv4Stack=true",
  "--enable-native-access=ALL-UNNAMED",
)

// JDK 24+: suppress sun.misc.Unsafe warnings emitted by upstream libs
// (scala-library, netty-common). The flag is unrecognized on Java 21 (the
// default), so only add it when running on a newer JDK.
javaOptions ++= {
  if (sys.props("java.specification.version").toInt >= 24) Seq("--sun-misc-unsafe-memory-access=allow")
  else Seq.empty
}

licenses := Seq("MIT License" -> uri("https://opensource.org/licenses/MIT"))

homepage := Some(uri("https://github.com/jamesward/zio-mavencentral"))

developers := List(
  Developer(
    "jamesward",
    "James Ward",
    "james@jamesward.com",
    uri("https://jamesward.com")
  )
)

versionScheme := Some("semver-spec")
