package com.jamesward.zio_mavencentral

import MavenCentral.{*, given}
import zio.*
import zio.direct.*
import zio.http.{Client, Path, URL, ZClientAspect}
import zio.test.*

import java.nio.file.Files
import java.time.ZonedDateTime

object MavenCentralSpec extends ZIOSpecDefault:

  given CanEqual[String, String] = CanEqual.derived
  given CanEqual[Seq[MavenCentral.ArtifactId], Seq[MavenCentral.ArtifactId]] = CanEqual.derived
  given CanEqual[Exit[MavenCentral.NotFoundError | Throwable, ?], Exit[MavenCentral.NotFoundError | Throwable, ?]] = CanEqual.derived
  given CanEqual[MavenCentral.Deploy.DeploymentState, MavenCentral.Deploy.DeploymentState] = CanEqual.derived

  def spec = suite("MavenCentral")(
    suite("artifacts")(
      test("artifactPath"):
        assertTrue(
          artifactPath(GroupId("org.webjars")) == Path.decode("org/webjars"),
          artifactPath(GroupId("org.webjars"), Some(ArtifactAndVersion(ArtifactId("jquery")))) == Path.decode("org/webjars/jquery"),
          artifactPath(GroupId("org.webjars"), Some(ArtifactAndVersion(ArtifactId("jquery"), Some(Version("3.6.4"))))) == Path.decode("org/webjars/jquery/3.6.4")
        )
      ,
      test("searchArtifacts"):
        defer:
          val webjarArtifacts = searchArtifacts(GroupId("org.webjars")).run.value
          val webjarNpmArtifacts = searchArtifacts(GroupId("org.webjars.npm")).run.value
          val springdataArtifacts = searchArtifacts(GroupId("org.springframework.data")).run.value
          val err = searchArtifacts(GroupId("zxcv12313asdf")).flip.run

          assertTrue(
            webjarArtifacts.size > 1000,
            !webjarNpmArtifacts.contains(ArtifactId("3.10.9")),
            springdataArtifacts.size > 10,
            err.isInstanceOf[GroupIdNotFoundError],
          )
      ,
      test("searchVersions"):
        defer:
          val versions = searchVersions(GroupId("org.webjars"), ArtifactId("jquery")).run.value
          val err = searchVersions(GroupId("com.jamesward"), ArtifactId("zxcvasdf")).flip.run

          assertTrue(
            versions.contains("3.6.4"),
            versions.indexOf(Version("1.12.4")) < versions.indexOf(Version("1.5.2")),
            err.isInstanceOf[GroupIdOrArtifactIdNotFoundError],
          )
      ,
      test("searchVersions does not change versions"):
        defer:
          val versions = searchVersions(GroupId("io.jenkins.archetypes"), ArtifactId("archetypes-parent")).run.value
          assertTrue:
            versions.contains("1.21")
      ,
      test("isModifiedSince groupId"):
        defer:
          val artifacts = searchArtifacts(GroupId("org.webjars")).run
          val lastModified = artifacts.maybeLastModified.get

          val isModifiedSinceLastModified = isModifiedSince(lastModified, GroupId("org.webjars")).run

          val isModifiedSinceLongAgo = isModifiedSince(ZonedDateTime.now().minusYears(10), GroupId("org.webjars")).run

          assertTrue(
            !isModifiedSinceLastModified,
            isModifiedSinceLongAgo
          )
      ,
      test("isModifiedSince artifactId"):
        defer:
          val versions = searchVersions(GroupId("org.webjars"), ArtifactId("bootstrap")).run
          val lastModified = versions.maybeLastModified.get

          val isModifiedSinceLastModified = isModifiedSince(lastModified, GroupId("org.webjars"), Some(ArtifactId("bootstrap"))).run

          val isModifiedSinceLongAgo = isModifiedSince(ZonedDateTime.now().minusYears(10), GroupId("org.webjars"), Some(ArtifactId("bootstrap"))).run

          val isModifiedSinceFuture = isModifiedSince(ZonedDateTime.now().plusSeconds(1), GroupId("org.webjars"), Some(ArtifactId("bootstrap"))).run

          assertTrue(
            !isModifiedSinceLastModified,
            isModifiedSinceLongAgo,
            !isModifiedSinceFuture
          )
      ,
      test("latest"):
        defer:
          assertTrue(latest(GroupId("com.jamesward"), ArtifactId("travis-central-test")).run.get == Version("0.0.15"))
      ,
      test("isArtifact"):
        defer:
          assertTrue(
            isArtifact(GroupId("com.jamesward"), ArtifactId("travis-central-test")).run,
            !isArtifact(GroupId("org.springframework"), ArtifactId("data")).run,
            !isArtifact(GroupId("org.springframework"), ArtifactId("cloud")).run,
          )
      ,
      test("artifactExists"):
        defer:
          assertTrue(
            artifactExists(GroupId("com.jamesward"), ArtifactId("travis-central-test"), Version("0.0.15")).run,
            !artifactExists(GroupId("com.jamesward"), ArtifactId("travis-central-test"), Version("0.0.0")).run,
          )
      ,
      test("javadocUri"):
        defer:
          val doesExist = javadocUri(GroupId("org.webjars"), ArtifactId("webjars-locator-core"), Version("0.52")).run
          val doesNotExist = javadocUri(GroupId("com.jamesward"), ArtifactId("travis-central-test"), Version("0.0.15")).exit.run // todo: flip no worky?

          assertTrue(
            doesNotExist == Exit.fail(NotFoundError(GroupId("com.jamesward"), ArtifactId("travis-central-test"), Version("0.0.15"))),
            URL.decode("https://repo1.maven.org/maven2/org/webjars/webjars-locator-core/0.52/webjars-locator-core-0.52-javadoc.jar").contains(doesExist),
          )
      ,
      test("downloadAndExtractZipWithCacheInfo"):
        val url = URL.decode("https://repo1.maven.org/maven2/com/jamesward/travis-central-test/0.0.15/travis-central-test-0.0.15.jar").toOption.get
        val tmpFile = Files.createTempDirectory("test").nn.toFile.nn

        downloadAndExtractZip(url, tmpFile).as(
          assertTrue(
            tmpFile.list().nn.contains("META-INF"),
            tmpFile.toPath.resolve("META-INF/maven/com.jamesward/travis-central-test/pom.properties").toFile.length() == 118,
          )
        )
      ,
      test("downloadAndExtractZip"):
        val url = URL.decode("https://repo1.maven.org/maven2/com/jamesward/travis-central-test/0.0.15/travis-central-test-0.0.15.jar").toOption.get
        val tmpFile = Files.createTempDirectory("test-streaming").nn.toFile.nn

        downloadAndExtractZip(url, tmpFile).as(
          assertTrue(
            tmpFile.list().nn.contains("META-INF"),
            tmpFile.toPath.resolve("META-INF/maven/com.jamesward/travis-central-test/pom.properties").toFile.length() == 118,
          )
        )
      ,
      test("downloadAndExtractZip - kotlin dokka javadoc"):
        val url = URL.decode("https://repo1.maven.org/maven2/io/ktor/ktor-serialization-jvm/3.2.3/ktor-serialization-jvm-3.2.3-javadoc.jar").toOption.get
        val tmpFile = Files.createTempDirectory("test-streaming-kotlin").nn.toFile.nn

        downloadAndExtractZip(url, tmpFile).as(
          assertTrue(
            tmpFile.list().nn.contains("index.html"),
          )
        )
      @@ TestAspect.timeout(30.seconds) @@ TestAspect.withLiveClock
      ,
      test("downloadAndExtractZip - 20 parallel"):
        val urls = Seq(
          "https://repo1.maven.org/maven2/com/jamesward/zio-mavencentral_3/0.0.21/zio-mavencentral_3-0.0.21-javadoc.jar",
          "https://repo1.maven.org/maven2/dev/zio/zio_3/2.1.9/zio_3-2.1.9-javadoc.jar",
          "https://repo1.maven.org/maven2/dev/zio/zio_2.13/2.1.9/zio_2.13-2.1.9-javadoc.jar",
          "https://repo1.maven.org/maven2/io/ktor/ktor-io-jvm/3.2.3/ktor-io-jvm-3.2.3-javadoc.jar",
          "https://repo1.maven.org/maven2/io/ktor/ktor-http-jvm/3.2.3/ktor-http-jvm-3.2.3-javadoc.jar",
          "https://repo1.maven.org/maven2/io/ktor/ktor-utils-jvm/3.2.3/ktor-utils-jvm-3.2.3-javadoc.jar",
          "https://repo1.maven.org/maven2/io/ktor/ktor-serialization-jvm/3.2.3/ktor-serialization-jvm-3.2.3-javadoc.jar",
          "https://repo1.maven.org/maven2/io/ktor/ktor-client-core-jvm/3.2.3/ktor-client-core-jvm-3.2.3-javadoc.jar",
          "https://repo1.maven.org/maven2/io/ktor/ktor-events-jvm/3.2.3/ktor-events-jvm-3.2.3-javadoc.jar",
          "https://repo1.maven.org/maven2/io/ktor/ktor-websockets-jvm/3.2.3/ktor-websockets-jvm-3.2.3-javadoc.jar",
          "https://repo1.maven.org/maven2/org/springframework/ai/spring-ai-mcp/1.0.1/spring-ai-mcp-1.0.1-javadoc.jar",
          "https://repo1.maven.org/maven2/com/vaadin/vaadin-confirm-dialog-flow/24.9.0/vaadin-confirm-dialog-flow-24.9.0-javadoc.jar",
          "https://repo1.maven.org/maven2/org/webjars/webjars-locator-lite/1.1.3/webjars-locator-lite-1.1.3-javadoc.jar",
          "https://repo1.maven.org/maven2/org/webjars/webjars-locator-core/0.52/webjars-locator-core-0.52-javadoc.jar",
          "https://repo1.maven.org/maven2/org/jsoup/jsoup/1.22.2/jsoup-1.22.2-javadoc.jar",
          "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.17/slf4j-simple-2.0.17-javadoc.jar",
          "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17-javadoc.jar",
          "https://repo1.maven.org/maven2/dev/zio/zio-schema_3/1.8.3/zio-schema_3-1.8.3-javadoc.jar",
          "https://repo1.maven.org/maven2/dev/zio/zio-streams_3/2.1.25/zio-streams_3-2.1.25-javadoc.jar",
          "https://repo1.maven.org/maven2/dev/zio/zio-test_3/2.1.25/zio-test_3-2.1.25-javadoc.jar",
        ).map(s => URL.decode(s).toOption.get)

        ZIO.foreachPar(urls): url =>
          val tmpFile = Files.createTempDirectory("test-parallel").nn.toFile.nn
          downloadAndExtractZip(url, tmpFile)
        .as(assertCompletes)
      @@ TestAspect.timeout(1.minutes) @@ TestAspect.withLiveClock
      ,
      // note that on some networks all DNS requests are accepted and redirect to something like a captive portal, wtf
      test("requestWithFallbackurl"):
        val artifactUrl = URL.decode("https://zxcvasdf123124zxcv.com/").toOption.get
        val fallbackArtifactUrl = URL.decode("https://repo1.maven.org/maven2/").toOption.get
        // bug in zio-direct:
        // assertTrue(response.status.isSuccess)
        // Exception occurred while executing macro expansion.
        // java.lang.Exception: Expected an expression. This is a partially applied Term. Try eta-expanding the term first.
        Client.requestWithFallback(Path.decode("com/jamesward/maven-metadata.xml"), primaryBaseUrl = artifactUrl, fallbackBaseUrl = fallbackArtifactUrl).map:
          (response, _) =>
            assertTrue(response.status.isSuccess)

      ,
      test("pom"):
        defer:
          val myPom = pom(GroupId("com.jamesward"), ArtifactId("zio-mavencentral_3"), Version("0.1.1")).run

          assertTrue(
            (myPom \ "name").text == "zio-mavencentral"
          )
      ,
      test("maven-metadata"):
        defer:
          val myMavenMetadata = mavenMetadata(GroupId("com.jamesward"), ArtifactId("zio-mavencentral_3")).run

          assertTrue(
            (myMavenMetadata.value \ "groupId").text == "com.jamesward",
            myMavenMetadata.maybeLastModified.isDefined
          )

    ).provide(Client.default.update(_ @@ ZClientAspect.requestLogging())),
    suite("deploy")(
      test("fail verification"):
        val filename = "momentjs-exists.zip"
        val zip = getClass.getResourceAsStream(s"/$filename").nn.readAllBytes()

        defer:
          val deploymentId = MavenCentral.Deploy.upload(filename, zip).debug.run

          val status = MavenCentral.Deploy.checkStatus(deploymentId)
            .filterOrFail(_.isFinal)(IllegalStateException("Waiting on final deployment status")) // todo: add current state to error
            .retry(Schedule.exponential(1.second))
            .ensuring(MavenCentral.Deploy.drop(deploymentId).ignore)
            .debug
            .run

          assertTrue(status == MavenCentral.Deploy.DeploymentState.FAILED)
      , test("upload and verify"):
        val filename = "momentjs-valid.zip"
        val zip = getClass.getResourceAsStream(s"/$filename").nn.readAllBytes()

        defer:
          val deploymentId = MavenCentral.Deploy.upload(filename, zip).debug.run

          val status = MavenCentral.Deploy.checkStatus(deploymentId)
            .filterOrFail(_.isFinal)(IllegalStateException("Waiting on final deployment status")) // todo: add current state to error
            .retry(Schedule.exponential(1.second))
            .ensuring(MavenCentral.Deploy.drop(deploymentId).ignore)
            .debug
            .run

          assertTrue(status == MavenCentral.Deploy.DeploymentState.VALIDATED)
//      , test("upload and publish"):
//        val filename = "tailwindcss-4.1.15.zip"
//        val zip = getClass.getResourceAsStream(s"/$filename").nn.readAllBytes()
//
//        defer:
//          MavenCentral.Deploy.uploadVerifyAndPublish(filename, zip).run
//          assertCompletes
    ).provide(Client.default.update(_ @@ ZClientAspect.requestLogging()), MavenCentral.Deploy.Sonatype.Live) @@ TestAspect.ifEnvSet("OSS_DEPLOY_USERNAME") @@ TestAspect.ifEnvSet("OSS_DEPLOY_PASSWORD") @@ TestAspect.withLiveSystem @@ TestAspect.withLiveClock
  )

