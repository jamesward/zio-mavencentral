package com.jamesward.zio_mavencentral

import MavenCentral.{*, given}
import zio.*
import zio.direct.*
import zio.http.{Body, Client, Handler, Header, MediaType, Method, Request, Response, Routes, Server, Status, URL, trailing}
import zio.stream.ZStream
import zio.test.*

import java.nio.file.Files

/**
 * The mirror policy of [[MavenCentral.MavenCentralRepo]], against a local
 * fake repo1 (authoritative, HTML listings), a fake GCS mirror, and a fake GCS
 * bucket-listing API. Each test sets what each mirror answers and checks who
 * was asked and what the caller got.
 */
object MirrorSpec extends ZIOSpecDefault:

  given CanEqual[String, String] = CanEqual.derived

  /** What a fake mirror answers for a path (relative to `maven2/`). */
  type Answer = (String, Request) => Response

  /** The fake mirrors: answers per mirror, and every request they got (mirror, path). */
  final case class Fakes(answers: Ref[Map[String, Answer]], seen: Ref[Chunk[(String, String)]], port: Int):
    def set(mirror: String)(answer: Answer): UIO[Unit] = answers.update(_ + (mirror -> answer))
    def asked(mirror: String): UIO[Chunk[String]] = seen.get.map(_.collect { case (m, p) if m == mirror => p })
    def reset: UIO[Unit] = answers.set(Map.empty) *> seen.set(Chunk.empty)
    def url(mirror: String): URL = URL.decode(s"http://localhost:$port/$mirror/maven2/").toOption.get

  private val fakesLayer: ZLayer[Server, Nothing, Fakes] =
    ZLayer.fromZIO:
      defer:
        val answers = Ref.make(Map.empty[String, Answer]).run
        val seen    = Ref.make(Chunk.empty[(String, String)]).run
        val handler = Handler.fromFunctionZIO[Request]: request =>
          val segments = request.path.segments
          val mirror   = segments.headOption.getOrElse("")
          // `/repo1/maven2/<path>`, `/gcs/maven2/<path>`, or `/api/<bucket>/o?prefix=…`
          val path     = if mirror == "api" then request.url.queryParam("prefix").getOrElse("") else segments.drop(2).mkString("/") + (if request.path.hasTrailingSlash then "/" else "")
          seen.update(_ :+ (mirror -> path)) *> answers.get.map(_.get(mirror).fold(Response.status(Status.NotFound))(_(path, request)))
        val port = Server.install(Routes(Method.ANY / trailing -> handler)).run
        Fakes(answers, seen, port)

  private def repoLayer(rateLimit: RateLimit = RateLimit()): ZLayer[Client & Fakes, Nothing, MavenCentralRepo] =
    // A short breaker window, so a test can trip it.
    ZLayer.service[Fakes].flatMap: env =>
      val fakes = env.get
      MavenCentralRepo.custom(
        mirrors = List(
          Mirror(fakes.url("repo1"), authoritative = true, Listings.Html),
          Mirror(fakes.url("gcs"), authoritative = false,
            Listings.GcsBucket(URL.decode(s"http://localhost:${fakes.port}/api/").toOption.get, "maven-central", "maven2/")),
        ),
        requestTimeout = 2.seconds,
        rateLimit = rateLimit,
        breaker = BreakerSettings(window = 1.second, minCalls = 5),
      )

  private val metadataXml =
    "<metadata><groupId>com.foo</groupId><artifactId>bar</artifactId><versioning><versions><version>1.0</version><version>1.1</version></versions></versioning></metadata>"

  private def ok(body: String): Answer = (_, _) => Response.text(body)
  private def status(s: Status): Answer = (_, _) => Response.status(s)

  private val ga = GroupArtifact(GroupId("com.foo"), ArtifactId("bar"))

  private def failure[E](exit: Exit[E, ?]): Option[E] = exit.causeOption.flatMap(_.failureOption)

  private def withRepo[E, A](rateLimit: RateLimit = RateLimit())(effect: ZIO[MavenCentralRepo & Client, E, A]): ZIO[Fakes & Client, E, A] =
    ZIO.serviceWithZIO[Fakes](_.reset) *> effect.provideSome[Fakes & Client](repoLayer(rateLimit))

  def spec = suite("Mirrors")(
    suite("metadata (repo1 first)")(
      test("repo1 answers; GCS isn't asked") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val meta = withRepo()(fakes.set("repo1")(ok(metadataXml)) *> fakes.set("gcs")(ok(metadataXml)) *> mavenMetadata(ga.groupId, ga.artifactId)).run
          assertTrue(meta.servedBy.exists(_.encode.contains("/repo1/")), fakes.asked("gcs").run.isEmpty)
      },
      test("repo1 failing: GCS answers") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val meta = withRepo()(fakes.set("repo1")(status(Status.ServiceUnavailable)) *> fakes.set("gcs")(ok(metadataXml)) *> mavenMetadata(ga.groupId, ga.artifactId)).run
          assertTrue(meta.servedBy.exists(_.encode.contains("/gcs/")), versionsOf(meta.value) == Seq(Version("1.1"), Version("1.0")))
      },
      test("repo1 failing and GCS doesn't have it: temporary, not 'not found'") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val (meta, latest) = withRepo()(fakes.set("repo1")(status(Status.ServiceUnavailable)) *> (mavenMetadata(ga.groupId, ga.artifactId).exit <*> latestOrFail(ga).exit)).run
          assertTrue(
            failure(meta).exists(_.isInstanceOf[NotFoundOnFallback]),
            failure(latest).exists(_.isInstanceOf[LatestUnavailable]),
          )
      },
      test("repo1's 404 is final; GCS isn't asked") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val meta = withRepo()(fakes.set("gcs")(ok(metadataXml)) *> mavenMetadata(ga.groupId, ga.artifactId).exit).run
          assertTrue(failure(meta).exists(_.isInstanceOf[GroupIdOrArtifactIdNotFoundError]), fakes.asked("gcs").run.isEmpty)
      },
      test("an unchanged artifact revalidates with a 304") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val etag  = "\"abc\""
          val answer: Answer = (_, request) =>
            if request.headers.get(Header.IfNoneMatch.name).contains(etag) then Response.status(Status.NotModified)
            else Response.text(metadataXml).addHeader(Header.Custom("ETag", etag))
          val (first, second) = withRepo()(
            fakes.set("repo1")(answer) *>
              (mavenMetadataIfModified(ga.groupId, ga.artifactId, None, None) <*>
                mavenMetadataIfModified(ga.groupId, ga.artifactId, Some(Header.ETag.Strong("abc")), None))
          ).run
          assertTrue(first.flatMap(_.maybeEtag).isDefined, second.isEmpty)
      },
    ),
    suite("jars (GCS first)")(
      test("GCS has it: repo1 isn't asked") {
        defer:
          val fakes  = ZIO.service[Fakes].run
          val target = ZIO.attemptBlockingIO(Files.createTempFile("mirror-spec", ".jar").nn.toFile).orDie.run
          withRepo()(fakes.set("gcs")(ok("jar bytes")) *> JarCache.mavenCentralDownloader("javadoc")(GroupArtifactVersion(ga.groupId, ga.artifactId, Version("1.0")), target)).run
          val bytes = ZIO.attemptBlockingIO(Files.readString(target.toPath).nn).orDie.run
          assertTrue(bytes == "jar bytes", fakes.asked("repo1").run.isEmpty, fakes.asked("gcs").run == Chunk("com/foo/bar/1.0/bar-1.0-javadoc.jar"))
      },
      test("too new for GCS: repo1 serves it") {
        defer:
          val fakes  = ZIO.service[Fakes].run
          val target = ZIO.attemptBlockingIO(Files.createTempFile("mirror-spec", ".jar").nn.toFile).orDie.run
          withRepo()(fakes.set("repo1")(ok("from repo1")) *> JarCache.mavenCentralDownloader("javadoc")(GroupArtifactVersion(ga.groupId, ga.artifactId, Version("1.0")), target)).run
          val bytes = ZIO.attemptBlockingIO(Files.readString(target.toPath).nn).orDie.run
          assertTrue(bytes == "from repo1")
      },
      test("neither has it: not found; GCS missing and repo1 failing: unavailable") {
        defer:
          val fakes  = ZIO.service[Fakes].run
          val target = ZIO.attemptBlockingIO(Files.createTempFile("mirror-spec", ".jar").nn.toFile).orDie.run
          val gav    = GroupArtifactVersion(ga.groupId, ga.artifactId, Version("1.0"))
          val missing = withRepo()(JarCache.mavenCentralDownloader("javadoc")(gav, target).exit).run
          val failing = withRepo()(fakes.set("repo1")(status(Status.BadGateway)) *> JarCache.mavenCentralDownloader("javadoc")(gav, target).exit).run
          assertTrue(
            failure(missing).exists(_.isInstanceOf[NotFoundError]),
            failure(failing).exists(_.isInstanceOf[JarCache.UpstreamUnavailableError]),
          )
      },
      test("a body that stalls fails as unavailable and leaves no jar; a slow, steady one finishes") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val gav   = GroupArtifactVersion(ga.groupId, ga.artifactId, Version("1.0"))
          // 1 KB, then nothing more.
          val stalled: Answer = (_, _) =>
            Response(body = Body.fromStream(ZStream.fromChunk(Chunk.fill(1024)(1.toByte)) ++ ZStream.never, 4096L))
          // 2 KB in 10 pieces 200 ms apart: 2 s in all, but never 500 ms idle.
          val steady: Answer = (_, _) =>
            Response(body = Body.fromStream(ZStream.fromIterable(List.fill(10)(Chunk.fill(205)(2.toByte))).schedule(Schedule.spaced(200.millis)).flattenChunks, 2050L))
          val dir = ZIO.attemptBlockingIO(Files.createTempDirectory("mirror-spec-stall").nn.toFile).orDie.run
          val (took, stall) = withRepo():
            fakes.set("gcs")(stalled) *>
              ZIO.scoped(JarCache.make(dir, JarCache.mavenCentralDownloader("javadoc", idleTimeout = 500.millis)).flatMap(_.get(gav))).exit
          .timed.run
          val leftover = ZIO.attemptBlockingIO(dir.listFiles.nn.toList.filter(_.getName.endsWith(".jar"))).orDie.run
          val target = ZIO.attemptBlockingIO(Files.createTempFile("mirror-spec", ".jar").nn.toFile).orDie.run
          withRepo()(fakes.set("gcs")(steady) *> JarCache.mavenCentralDownloader("javadoc", idleTimeout = 500.millis)(gav, target)).run
          val size = ZIO.attemptBlockingIO(target.length()).orDie.run
          assertTrue(
            failure(stall).exists(_.isInstanceOf[JarCache.UpstreamUnavailableError]),
            took < 10.seconds,
            leftover.isEmpty,
            size == 2050L,
          )
      },
      test("artifactExists checks the POM, which GCS has") {
        defer:
          val fakes  = ZIO.service[Fakes].run
          val exists = withRepo()(fakes.set("gcs")((p, _) => if p.endsWith("bar-1.0.pom") then Response.ok else Response.notFound) *> artifactExists(ga.groupId, ga.artifactId, Version("1.0"))).run
          assertTrue(exists, fakes.asked("repo1").run.isEmpty)
      },
    ),
    suite("listings")(
      test("repo1's HTML listing; GCS's bucket listing when repo1 fails; empty on GCS isn't 'not found'") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val html  = "<html><body>\n<a href=\"bar/\">bar/</a>\n<a href=\"baz/\">baz/</a>\n</body></html>"
          val fromRepo1 = withRepo()(fakes.set("repo1")((p, _) => if p == "com/foo/" then Response.text(html).contentType(MediaType.text.html) else Response.notFound) *> searchArtifacts(ga.groupId)).run
          val fromGcs = withRepo()(
            fakes.set("repo1")(status(Status.ServiceUnavailable)) *>
              fakes.set("api")((_, _) => Response.json("""{"prefixes":["maven2/com/foo/bar/","maven2/com/foo/qux/"]}""")) *>
              searchArtifacts(ga.groupId)
          ).run
          val emptyOnGcs = withRepo()(
            fakes.set("repo1")(status(Status.ServiceUnavailable)) *> fakes.set("api")((_, _) => Response.json("{}")) *> searchArtifacts(ga.groupId).exit
          ).run
          val absent = withRepo()(searchArtifacts(ga.groupId).exit).run
          assertTrue(
            fromRepo1.value == Seq(ArtifactId("bar"), ArtifactId("baz")),
            fromGcs.value == Seq(ArtifactId("bar"), ArtifactId("qux")),
            fromGcs.servedBy.exists(_.encode.contains("/gcs/")),
            failure(emptyOnGcs).exists(_.isInstanceOf[NotFoundOnFallback]),
            failure(absent).exists(_.isInstanceOf[GroupIdNotFoundError]),
          )
      },
      test("parseBucketPage reads prefixes and the next page token") {
        val (names, token) = parseBucketPage("maven2/com/foo/")("""{"nextPageToken":"t1","prefixes":["maven2/com/foo/a/","maven2/com/foo/b_2.13/"]}""")
        assertTrue(names == Seq("a", "b_2.13"), token.contains("t1"))
      },
    ),
    suite("repo1 budget and breakers")(
      test("over budget: GCS answers what it has; a 'not found' still needs repo1") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val result = withRepo(RateLimit(perSecond = 0.001, burst = 1, authoritativeWait = 100.millis)):
            defer:
              fakes.set("repo1")(ok(metadataXml)).run
              fakes.set("gcs")((p, _) => if p.startsWith("com/foo/bar/") then Response.text(metadataXml) else Response.notFound).run
              val first    = mavenMetadata(ga.groupId, ga.artifactId).run                       // repo1's one token
              val second   = mavenMetadata(ga.groupId, ga.artifactId).run                       // over budget: GCS
              val unknown  = mavenMetadata(ga.groupId, ArtifactId("new")).exit.run              // GCS 404, no budget to ask repo1
              val limited  = ZIO.serviceWithZIO[MavenCentralRepo](_.rateLimited).run
              (first, second, unknown, limited)
          .run
          val (first, second, unknown, limited) = result
          assertTrue(
            first.servedBy.exists(_.encode.contains("/repo1/")),
            second.servedBy.exists(_.encode.contains("/gcs/")),
            failure(unknown).exists(_.isInstanceOf[NotFoundOnFallback]),
            limited >= 2L,
          )
      },
      test("a failing repo1 trips its breaker: requests stop going to it, and the change is logged") {
        defer:
          val fakes = ZIO.service[Fakes].run
          withRepo():
            fakes.set("repo1")(status(Status.ServiceUnavailable)) *> fakes.set("gcs")(ok(metadataXml)) *>
              ZIO.foreachDiscard(1 to 30)(_ => mavenMetadata(ga.groupId, ga.artifactId).delay(50.millis)) *>
              fakes.seen.set(Chunk.empty) *>
              ZIO.foreachDiscard(1 to 5)(_ => mavenMetadata(ga.groupId, ga.artifactId))
          .run
          val repo1After = fakes.asked("repo1").run
          val logged     = ZTestLogger.logOutput.run.exists(_.message() == "Mirror circuit breaker changed state")
          assertTrue(repo1After.isEmpty, logged)
      },
      test("every mirror down: temporary") {
        defer:
          val fakes = ZIO.service[Fakes].run
          val latest = withRepo()(fakes.set("repo1")(status(Status.ServiceUnavailable)) *> fakes.set("gcs")(status(Status.ServiceUnavailable)) *> latestOrFail(ga).exit).run
          assertTrue(failure(latest).exists(_.isInstanceOf[LatestUnavailable]))
      },
    ),
  ).provide(Server.defaultWith(_.onAnyOpenPort), Client.default, fakesLayer) @@ TestAspect.withLiveClock @@ TestAspect.sequential @@ TestAspect.timeout(2.minutes)
