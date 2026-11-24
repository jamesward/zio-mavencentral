package com.jamesward.zio_mavencentral

import MavenCentral.{*, given}
import zio.*
import zio.direct.*
import zio.http.{Client, Handler, Method, Path, Response, Routes, Server, Status, URL, ZClientAspect}
import zio.stream.ZStream
import zio.test.*

import java.nio.file.Files
import java.time.ZonedDateTime

object MavenCentralSpec extends ZIOSpecDefault:

  given CanEqual[String, String] = CanEqual.derived
  given CanEqual[Seq[MavenCentral.ArtifactId], Seq[MavenCentral.ArtifactId]] = CanEqual.derived
  given CanEqual[Exit[MavenCentral.NotFoundError | Throwable, ?], Exit[MavenCentral.NotFoundError | Throwable, ?]] = CanEqual.derived
  given CanEqual[MavenCentral.Deploy.DeploymentState, MavenCentral.Deploy.DeploymentState] = CanEqual.derived

  private def failure[E](exit: Exit[E, ?]): Option[E] = exit.causeOption.flatMap(_.failureOption)

  /** The fake mirror answers with the status named by the first path segment (`/404/maven2/...`). */
  private def statusFor(request: zio.http.Request): Response =
    Response.status(request.path.segments.headOption.flatMap(_.toIntOption).map(Status.fromInt).getOrElse(Status.InternalServerError))

  private def fakeRepo(port: Int, status: Status): ZLayer[Client, Nothing, MavenCentralRepo] =
    MavenCentralRepo.custom(mirrors = List(Mirror(URL.decode(s"http://localhost:$port/${status.code}/maven2/").toOption.get, authoritative = true, Listings.Html)))

  /** Every lookup maps `status` to its not-found error. */
  private def notFoundEverywhere(port: Int, status: Status): ZIO[Client, Nothing, Boolean] =
    val ga = GroupArtifact(GroupId("com.foo"), ArtifactId("bar"))
    (mavenMetadata(ga.groupId, ga.artifactId).exit
      <*> latestOrFail(ga).exit
      <*> javadocUri(ga.groupId, ga.artifactId, Version("1.0")).exit
      <*> pom(ga.groupId, ga.artifactId, Version("1.0")).exit
      <*> searchArtifacts(ga.groupId).exit
      <*> isModifiedSince(ZonedDateTime.now().nn, ga.groupId, Some(ga.artifactId)).exit
    ).provideSome[Client](fakeRepo(port, status)).map: (meta, latest, url, pomExit, group, modified) =>
      failure(meta).exists(_.isInstanceOf[GroupIdOrArtifactIdNotFoundError])
        && failure(latest).exists(_.isInstanceOf[GroupIdOrArtifactIdNotFoundError])
        && failure(url).exists(_.isInstanceOf[NotFoundError])
        && failure(pomExit).exists(_.isInstanceOf[NotFoundError])
        && failure(group).exists(_.isInstanceOf[GroupIdNotFoundError])
        && failure(modified).exists(_.isInstanceOf[GroupIdOrArtifactIdNotFoundError])

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
      test("coordinates Maven Central rejects with 400 are not found, not errors"):
        // repo1.maven.org answers 400 for a NUL or invalid UTF-8 in a segment,
        // and for a segment over ~1,100 characters (issue javadoccentral#270).
        val malformed = List("x\u0000", "x\uFFFD\u0000", "a" * 1200)
        defer:
          val results = ZIO.foreach(malformed): bad =>
            defer:
              val ga     = GroupArtifact(GroupId("com.foo"), ArtifactId(bad))
              val meta   = mavenMetadata(GroupId("com.foo"), ArtifactId(bad)).exit.run
              val latest = latestOrFail(ga).exit.run
              val url    = javadocUri(GroupId("com.foo"), ArtifactId(bad), Version("1.0")).exit.run
              val group  = searchArtifacts(GroupId(bad)).exit.run
              (meta, latest, url, group)
          .run
          assertTrue(results.forall: (meta, latest, url, group) =>
            meta.causeOption.flatMap(_.failureOption).exists(_.isInstanceOf[GroupIdOrArtifactIdNotFoundError])
              && latest.causeOption.flatMap(_.failureOption).exists(_.isInstanceOf[GroupIdOrArtifactIdNotFoundError])
              && url.causeOption.flatMap(_.failureOption).exists(_.isInstanceOf[NotFoundError])
              && group.causeOption.flatMap(_.failureOption).exists(_.isInstanceOf[GroupIdNotFoundError])
          )
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
            // An immutable file: served by GCS when it has it (it does for an old release).
            URL.decode(s"${gcsMirror.encode}org/webjars/webjars-locator-core/0.52/webjars-locator-core-0.52-javadoc.jar").contains(doesExist),
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
      test("MavenCentralRepo falls over from a bad mirror to a working one"):
        // An unreachable authoritative mirror, then the real repo1 as a fallback.
        val badMirror = URL.decode("https://zxcvasdf123124zxcv.com/").toOption.get
        val goodMirror = URL.decode("https://repo1.maven.org/maven2/").toOption.get
        val customRepo = MavenCentralRepo.custom(
          mirrors = List(Mirror(badMirror, authoritative = true, Listings.Html), Mirror(goodMirror, authoritative = false, Listings.Unsupported)),
        )

        ZIO.serviceWithZIO[MavenCentralRepo]: repo =>
          repo.request(Path.decode("com/jamesward/maven-metadata.xml")).map:
            (response, _) =>
              assertTrue(response.status.isSuccess)
        .provideSome[Client](customRepo)
      ,
      test("with repo1 unreachable, a group listing comes from GCS's bucket API"):
        val unreachable = URL.decode("https://zxcvasdf123124zxcv.com/maven2/").toOption.get
        val repo = MavenCentralRepo.custom(mirrors = List(Mirror(unreachable, authoritative = true, Listings.Html), gcsMirrorSpec))
        defer:
          val fromGcs   = searchArtifacts(GroupId("com.jamesward")).provideSome[Client](repo).run
          val fromRepo1 = searchArtifacts(GroupId("com.jamesward")).run
          assertTrue(
            fromGcs.servedBy.exists(_.encode.startsWith(gcsMirror.encode)),
            fromGcs.value.contains(ArtifactId("zio-mavencentral_3")),
            fromRepo1.value.toSet.subsetOf(fromGcs.value.toSet) || fromGcs.value.toSet.subsetOf(fromRepo1.value.toSet),
          )
      ,
      test("the default mirrors serve a javadoc jar from GCS"):
        defer:
          val target = ZIO.attemptBlockingIO(Files.createTempFile("gcs-jar", ".jar").nn.toFile).orDie.run
          JarCache.mavenCentralDownloader("javadoc")(gav("org.webjars", "webjars-locator-core", "0.52"), target).run
          val size = ZIO.attemptBlockingIO(target.length()).orDie.run
          assertTrue(size > 10_000L)
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

    ).provide(Client.default.update(_ @@ ZClientAspect.requestLogging()), MavenCentralRepo.live),
    suite("upstream statuses (a local fake mirror)")(
      // The fake mirror answers every request with one status, so each mapping
      // is tested whatever Maven Central does on the day.
      test("404, 400 and 414 are not found everywhere"):
        defer:
          val port = Server.install(Routes(Method.ANY / zio.http.trailing -> Handler.fromFunction[zio.http.Request](statusFor))).run
          val results = ZIO.foreach(List(Status.NotFound, Status.BadRequest, Status.RequestUriTooLong))(notFoundEverywhere(port, _)).run
          assertTrue(results.forall(identity))
      ,
      test("latestOrFail types an unreachable upstream as LatestUnavailable, not a defect"):
        defer:
          val port = Server.install(Routes(Method.ANY / zio.http.trailing -> Handler.fromFunction[zio.http.Request](statusFor))).run
          val ga   = GroupArtifact(GroupId("com.foo"), ArtifactId("bar"))
          val exit = latestOrFail(ga).exit.provideSome[Client](fakeRepo(port, Status.ServiceUnavailable)).run
          assertTrue(failure(exit).exists:
            case LatestUnavailable(g, reason) => g == ga && reason.contains("503")
            case _                            => false
          )
    ).provide(Server.defaultWith(_.onAnyOpenPort), Client.default) @@ TestAspect.withLiveClock @@ TestAspect.sequential,
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
          val deploymentId = MavenCentral.Deploy.upload(
            filename,
            ZStream.fromChunk(Chunk.fromArray(zip)),
          ).debug.run

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

