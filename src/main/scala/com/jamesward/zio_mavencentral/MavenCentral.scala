package com.jamesward.zio_mavencentral

import org.bouncycastle.bcpg.{ArmoredOutputStream, BCPGOutputStream, HashAlgorithmTags}
import org.bouncycastle.openpgp.operator.jcajce.{JcaKeyFingerprintCalculator, JcaPGPContentSignerBuilder, JcePBESecretKeyDecryptorBuilder}
import org.bouncycastle.openpgp.{PGPPrivateKey, PGPPublicKey, PGPSecretKeyRing, PGPSignature, PGPSignatureGenerator}
import zio.direct.*
import zio.http.*
import zio.http.codec.PathCodec
import zio.schema.{Schema, derived}
import zio.stream.{ZPipeline, ZSink}
import zio.{Chunk, ChunkBuilder, IO, Schedule, Scope, Trace, ZIO, ZLayer, durationInt}

import java.io.{File, IOException}
import java.nio.file.Files
import java.time.ZonedDateTime
import java.util.Base64
import scala.annotation.{targetName, unused}
import scala.util.matching.Regex
import scala.xml.Elem

object MavenCentral:

  // todo: regionalize to maven central mirrors for lower latency
  private val artifactUri = URL.decode("https://repo1.maven.org/maven2/").toOption.get
  private val fallbackArtifactUri = URL.decode("https://repo.maven.apache.org/maven2/").toOption.get

  opaque type GroupId = String
  object GroupId:
    def apply(s: String): GroupId = s
    def unapply(s: String): Option[GroupId] = Some(s)

  extension (groupId: GroupId)
    @targetName("slash")
    def /(artifactId: ArtifactId): Path = Path.root / groupId / artifactId

  opaque type ArtifactId = String
  object ArtifactId:
    def apply(s: String): ArtifactId = s
    def unapply(s: String): Option[ArtifactId] = Some(s)

  opaque type Version = String
  object Version:
    def apply(s: String): Version = s
    def unapply(s: String): Option[Version] = Some(s)
    val latest: Version = Version("latest")

  case class ArtifactAndVersion(artifactId: ArtifactId, maybeVersion: Option[Version] = None)
  case class GroupArtifact(groupId: GroupId, artifactId: ArtifactId):
    lazy val toPath: Path = groupId / artifactId

    @targetName("slash")
    @unused
    def /(version: Version): Path = toPath / version

  case class GroupArtifactVersion(groupId: GroupId, artifactId: ArtifactId, version: Version):
    @unused
    lazy val noVersion: GroupArtifact = GroupArtifact(groupId, artifactId)
    @unused
    lazy val toPath: Path = groupId / artifactId / version
    override def toString: String = s"$groupId/$artifactId/$version"

  case class WithCacheInfo[A](value: A, maybeLastModified: Option[ZonedDateTime], maybeEtag: Option[Header.ETag])

  /**
   * Cache metadata extracted from an HTTP response when downloading a jar.
   * Useful for implementing conditional `If-None-Match` / `If-Modified-Since`
   * requests against Maven Central without re-fetching the body.
   */
  case class JarMeta(maybeLastModified: Option[ZonedDateTime], maybeEtag: Option[Header.ETag])

  case class GroupIdNotFoundError(groupId: GroupId)
  case class GroupIdOrArtifactIdNotFoundError(groupId: GroupId, artifactId: ArtifactId)
  case class NotFoundError(groupId: GroupId, artifactId: ArtifactId, version: Version)
  /** No published versions for this `GroupArtifact` (Maven Central returned an
   *  empty version list). Surfaced by [[latestOrFail]]. */
  case class LatestNotFound(groupArtifact: GroupArtifact)
  /** A transient HTTP failure that's worth retrying — 5xx server errors *or*
   *  429 Too Many Requests (Maven Central throttles aggressive clients with
   *  429s; treating them as retryable lets [[retryOnServerError]] back off). */
  case class TemporaryServerError(response: Response) extends Throwable(s"${response.status.code}: ${response.status.text}")
  private case class UnknownError(response: Response) extends Throwable(response.status.text)
  private case class ParseError(t: Throwable) extends Throwable(t)

  // Promote a non-success response to either a TemporaryServerError (retryable)
  // or an UnknownError (terminal). Centralizing the rule means new retryable
  // status codes only need to be added in one place.
  private def temporaryOrUnknown(response: Response): Throwable =
    if response.status.isServerError || response.status.code == 429 then TemporaryServerError(response)
    else UnknownError(response)

  /** Convenience constructor: `gav("org.foo", "bar", "1.2.3")`. */
  def gav(groupId: String, artifactId: String, version: String): GroupArtifactVersion =
    GroupArtifactVersion(GroupId(groupId), ArtifactId(artifactId), Version(version))

  /**
   * Retries a ZIO effect up to 2 additional times on `TemporaryServerError`
   * (Maven Central 5xx) with exponential backoff starting at 1 second.
   * Permanent errors (e.g. `NotFoundError`, 4xx) fail immediately.
   *
   * Use:
   * {{{
   *   import com.jamesward.zio_mavencentral.MavenCentral.retryOnServerError
   *   MavenCentral.searchVersions(g, a).retryOnServerError
   * }}}
   */
  extension [R, E, A](zio: ZIO[R, E, A])
    def retryOnServerError: ZIO[R, E, A] =
      zio.retry(
        Schedule.recurWhile[E]:
          case _: TemporaryServerError => true
          case _                       => false
        && Schedule.exponential(1.second) && Schedule.recurs(2)
      )

  given Schema[GroupId] = Schema.primitive[String].transform(MavenCentral.GroupId.apply, identity)
  given Schema[ArtifactId] = Schema.primitive[String].transform(MavenCentral.ArtifactId.apply, identity)
  given Schema[Version] = Schema.primitive[String].transform(MavenCentral.Version.apply, identity)

  private def stringToGroupArtifact(s: String): MavenCentral.GroupArtifact =
    // todo: parse failure handling
    val parts = s.split(':')
    MavenCentral.GroupArtifact(MavenCentral.GroupId(parts(0)), MavenCentral.ArtifactId(parts(1)))

  private def groupArtifactToString(ga: MavenCentral.GroupArtifact): String = s"${ga.groupId}:${ga.artifactId}"

  given Schema[MavenCentral.GroupArtifact] = Schema.primitive[String].transform(stringToGroupArtifact, groupArtifactToString)

  given CanEqual[Path, Path] = CanEqual.derived
  given CanEqual[GroupId, GroupId] = CanEqual.derived
  given CanEqual[ArtifactId, ArtifactId] = CanEqual.derived
  given CanEqual[Version, Version] = CanEqual.derived
  given CanEqual[GroupArtifact, GroupArtifact] = CanEqual.derived
  given CanEqual[GroupArtifactVersion, GroupArtifactVersion] = CanEqual.derived
  given CanEqual[URL, URL] = CanEqual.derived
  given CanEqual[Status, Status] = CanEqual.derived


  object CaseInsensitiveOrdering extends Ordering[ArtifactId]:
    def compare(a: ArtifactId, b: ArtifactId): Int = a.compareToIgnoreCase(b)

  def artifactPath(groupId: GroupId, artifactAndVersion: Option[ArtifactAndVersion] = None): Path =
    val withGroup = groupId.split('.').foldLeft(Path.empty)(_ / _)
    val withArtifact = artifactAndVersion.fold(withGroup)(withGroup / _.artifactId)
    val withVersion = artifactAndVersion.flatMap(_.maybeVersion).fold(withArtifact)(withArtifact / _)
    withVersion

  private val filenameExtractor: Regex = """.*<a href="([^"]+)".*""".r

  private def lineExtractor(names: Seq[String], line: String): Seq[String] =
    line match
      case filenameExtractor(line) if line.endsWith("/") && !line.startsWith("..") =>
        names :+ line.stripSuffix("/")
      case _ =>
        names

  private def responseToNames(response: Response): ZIO[Any, ParseError, Seq[String]] =
    response.body.asStream
      .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
      .runFold(Seq.empty[String])(lineExtractor)
      .mapError(ParseError.apply)

  extension (client: Client.type)
    def requestWithFallback(
                                path: Path,
                                method: Method = Method.GET,
                                headers: Headers = Headers.empty,
                                content: Body = Body.empty,
                                primaryBaseUrl: URL = artifactUri,
                                fallbackBaseUrl: URL = fallbackArtifactUri
                              )(implicit trace: Trace): ZIO[Client, Throwable, (Response, URL)] =
      ZIO.serviceWithZIO[Client]:
        client =>
          val req = Request(method = method, url = primaryBaseUrl.addPath(path), headers = headers, body = content)
          client.batched(req).map(_ -> req.url).orElse:
            val fallbackReq = req.updateURL(_ => fallbackBaseUrl.addPath(path))
            client.batched(fallbackReq).map(_ -> fallbackReq.url)

  // When a groupId path is *also* an artifact path (e.g. org.webjars:npm lives
  // at /org/webjars/npm/, sibling to every artifact in the org.webjars.npm
  // group), the directory listing includes the artifact's version subdirs as
  // pseudo-artifacts. Fetch the maven-metadata.xml at the group path to find
  // out which subdir names are actually versions of the group-as-artifact.
  //
  // 404 ⇒ no metadata file at the group path (typical for pure groupIds like
  // org.springframework). Other errors (5xx / 429) propagate so the caller
  // can retry — previously these were silently swallowed via
  // `.orElseSucceed(Seq.empty)`, causing every version directory to leak
  // through as a fake artifact whenever Maven Central rate-limited us.
  private def versionsAtGroupPath(groupId: GroupId): ZIO[Client, TemporaryServerError | Throwable, Seq[Version]] =
    defer:
      val path = artifactPath(groupId) / "maven-metadata.xml"
      val (response, _) = Client.requestWithFallback(path, Method.GET).run
      response.status match
        case status if status.isSuccess =>
          val body = response.body.asString.run
          val xml  = scala.xml.XML.loadString(body)
          (xml \ "versioning" \ "versions" \ "version").map(n => Version(n.text)).toSeq
        case Status.NotFound =>
          ZIO.succeed(Seq.empty[Version]).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  // Removes any versions found in this groupId
  def searchArtifacts(groupId: GroupId): ZIO[Client, GroupIdNotFoundError | TemporaryServerError | Throwable, WithCacheInfo[Seq[ArtifactId]]] =
    val path = artifactPath(groupId).addTrailingSlash
    val allFilesReq = Client.requestWithFallback(path).map(_._1)
    allFilesReq.zipWithPar(versionsAtGroupPath(groupId)):
      (allFilesResp, versions) =>
        allFilesResp.status match
          case Status.NotFound =>
            ZIO.fail(GroupIdNotFoundError(groupId))
          case s if s.isSuccess =>
            responseToNames(allFilesResp).map:
              names =>
                WithCacheInfo(
                  names.diff(versions).map(ArtifactId(_)),
                  allFilesResp.header(Header.LastModified).map(_.value),
                  allFilesResp.header(Header.ETag)
                )
          case _ =>
            ZIO.fail(temporaryOrUnknown(allFilesResp))
    .flatten


  // newest first
  def searchVersions(groupId: GroupId, artifactId: ArtifactId): ZIO[Client, GroupIdOrArtifactIdNotFoundError | TemporaryServerError | Throwable, WithCacheInfo[Seq[Version]]] =
    defer:
      val metadata = mavenMetadata(groupId, artifactId).run
      val versions = metadata.value \ "versioning" \ "versions" \ "version"
      WithCacheInfo(
        versions.map(n => Version(n.text)).reverse,
        metadata.maybeLastModified,
        metadata.maybeEtag,
      )

  def isModifiedSince(dateTime: ZonedDateTime, groupId: GroupId, maybeArtifactId: Option[ArtifactId] = None):
    ZIO[Client, GroupIdNotFoundError | GroupIdOrArtifactIdNotFoundError | Throwable, Boolean] =
      defer:
        val path = maybeArtifactId.fold(artifactPath(groupId).addTrailingSlash)(artifactId => artifactPath(groupId, Some(ArtifactAndVersion(artifactId))) / "maven-metadata.xml")
        val (response, _) = Client.requestWithFallback(path, Method.HEAD, Headers(Header.IfModifiedSince(dateTime))).run
        response.status match
          case Status.Ok =>
            // workaround for MavenCentral exact time matching on file not working correctly
            if response.header(Header.LastModified).map(_.value).contains(dateTime) then
              ZIO.succeed(false).run
            else
              ZIO.succeed(true).run
          case Status.NotModified => ZIO.succeed(false).run
          case Status.NotFound => ZIO.fail(maybeArtifactId.fold(GroupIdNotFoundError(groupId))(GroupIdOrArtifactIdNotFoundError(groupId, _))).run
          case _ => ZIO.fail(temporaryOrUnknown(response)).run

  // todo: potentially use maven-metadata.xml
  def latest(groupId: GroupId, artifactId: ArtifactId): ZIO[Client, GroupIdOrArtifactIdNotFoundError | TemporaryServerError | Throwable, Option[Version]] =
    searchVersions(groupId, artifactId).map(_.value.headOption)

  /**
   * Resolve the latest published version for a `GroupArtifact`, with
   * `retryOnServerError` and a typed failure channel. Equivalent to:
   *   `latest(g, a).retryOnServerError.someOrFail(LatestNotFound(ga))`
   * with throwables `orDie`'d. Convenient for callers that want a clean
   * `ZIO[Client, GroupIdOrArtifactIdNotFoundError | LatestNotFound, Version]`.
   */
  def latestOrFail(groupArtifact: GroupArtifact):
      ZIO[Client, GroupIdOrArtifactIdNotFoundError | LatestNotFound, Version] =
    latest(groupArtifact.groupId, groupArtifact.artifactId)
      .retryOnServerError
      .catchAll:
        case t: Throwable                        => ZIO.die(t)
        case e: GroupIdOrArtifactIdNotFoundError => ZIO.fail(e)
      .someOrFail(LatestNotFound(groupArtifact))

  def isArtifact(groupId: GroupId, artifactId: ArtifactId): ZIO[Client, Throwable, Boolean] =
    defer:
      val metadata = mavenMetadata(groupId, artifactId).run
      val body = metadata.value.toString
      // checks that the maven-metadata.xml contains the groupId
      body.contains(s"<groupId>$groupId</groupId>")
    .catchAll:
      _ => ZIO.succeed(false)

  def artifactExists(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, Throwable, Boolean] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))).addTrailingSlash
      val (response, _) = Client.requestWithFallback(path, Method.HEAD).run
      response.status.isSuccess

  def pom(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, NotFoundError | TemporaryServerError | Throwable, Elem] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version.pom"
      val (response, url) = Client.requestWithFallback(path, Method.GET).run
      response.status match
        case status if status.isSuccess =>
          val body = response.body.asString.run
          scala.xml.XML.loadString(body)
        case Status.NotFound =>
          ZIO.fail(NotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  def mavenMetadata(groupId: GroupId, artifactId: ArtifactId): ZIO[Client, GroupIdOrArtifactIdNotFoundError | TemporaryServerError | Throwable, WithCacheInfo[Elem]] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId))) / "maven-metadata.xml"
      val (response, url) = Client.requestWithFallback(path, Method.GET).run
      response.status match
        case status if status.isSuccess =>
          val body = response.body.asString.run
          WithCacheInfo(
            scala.xml.XML.loadString(body),
            response.header(Header.LastModified).map(_.value),
            response.header(Header.ETag)
          )
        case Status.NotFound =>
          ZIO.fail(GroupIdOrArtifactIdNotFoundError(groupId, artifactId)).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  private def artifactUrl(groupId: GroupId, artifactId: ArtifactId, version: Version)(filename: String): ZIO[Client, NotFoundError | TemporaryServerError | Throwable, URL] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / filename
      val (response, url) = Client.requestWithFallback(path, Method.HEAD).run
      response.status match
        case status if status.isSuccess =>
          ZIO.succeed(url).run
        case Status.NotFound =>
          ZIO.fail(NotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  /** URL to the main `.jar` for a GAV. */
  def jarUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, NotFoundError | TemporaryServerError | Throwable, URL] =
    artifactUrl(groupId, artifactId, version)(s"$artifactId-$version.jar")

  def javadocUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, NotFoundError | TemporaryServerError | Throwable, URL] =
    artifactUrl(groupId, artifactId, version)(s"$artifactId-$version-javadoc.jar")

  def sourcesUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, NotFoundError | TemporaryServerError | Throwable, URL] =
    artifactUrl(groupId, artifactId, version)(s"$artifactId-$version-sources.jar")

  // Extract a downloaded zip file to a destination directory using java.util.zip.ZipFile
  // (random access, avoids the concurrency issues of streaming unarchivers under load).
  private def extractZipFile(zipPath: java.nio.file.Path, destination: File): ZIO[Any, Throwable, Set[String]] =
    ZIO.scoped:
      defer:
        val zipFile = ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(new java.util.zip.ZipFile(zipPath.toFile))).run
        ZIO.attemptBlockingIO:
          import scala.jdk.CollectionConverters.*
          zipFile.entries().nn.asScala.foldLeft(Set.empty[String]): (acc, entry) =>
            val targetPath = destination.toPath.resolve(entry.getName)
            if entry.isDirectory then
              Files.createDirectories(targetPath)
              acc
            else
              Files.createDirectories(targetPath.getParent)
              val is = zipFile.getInputStream(entry).nn
              try Files.copy(is, targetPath) finally is.close()
              acc + entry.getName.nn
        .run

  /**
   * Download a jar (or any binary blob) from `source` to `target`, returning
   * the HTTP cache metadata (`Last-Modified` / `ETag`).
   *
   * Streams the response body straight to disk via `ZSink.fromPath`; never
   * materializes the full body in memory.
   *
   *  - 5xx responses fail with `TemporaryServerError` (retry-friendly via
   *    `retryOnServerError`).
   *  - Other non-success responses fail with an internal `UnknownError`
   *    surfaced as a generic `Throwable`.
   */
  def downloadJar(source: URL, target: File): ZIO[Client, Throwable, JarMeta] =
    ZIO.scoped:
      defer:
        val response = Client.streaming(Request.get(source)).run
        response.status match
          case status if status.isError => ZIO.fail(temporaryOrUnknown(response)).run
          case _ => // success, continue
        response.body.asStream.run(ZSink.fromPath(target.toPath)).run
        JarMeta(
          response.header(Header.LastModified).map(_.value),
          response.header(Header.ETag),
        )

  // Download a zip to a temporary file, extract it to `destination`, then delete the tmp file.
  // The download step returns HTTP response headers for cache metadata extraction.
  private def downloadAndExtract(source: URL, destination: File): ZIO[Client, Throwable, (JarMeta, Set[String])] =
    ZIO.scoped:
      defer:
        val tmpFile = ZIO.acquireRelease(
          ZIO.attemptBlockingIO(Files.createTempFile("mvncentral-dl-", ".zip").nn.toFile)
        )(file => ZIO.attemptBlockingIO(Files.deleteIfExists(file.toPath)).ignoreLogged).run
        val meta  = downloadJar(source, tmpFile).run
        val files = extractZipFile(tmpFile.toPath, destination).run
        (meta, files)

  // Download and extract a zip, returning the list of extracted files along with
  // Maven Central cache metadata (Last-Modified / ETag) from the HTTP response.
  def downloadAndExtractZip(source: URL, destination: File): ZIO[Client, Throwable, WithCacheInfo[Set[String]]] =
    downloadAndExtract(source, destination).map: (meta, files) =>
      WithCacheInfo(files, meta.maybeLastModified, meta.maybeEtag)

  /**
   * `zio-http` `PathCodec` instances for the GAV value types. Convenient for
   * route definitions like `Method.GET / "files" / groupArtifactVersion / trailing`.
   */
  object Codecs:
    val groupId: PathCodec[GroupId] =
      PathCodec.string("groupId").transform(GroupId(_))(_.toString)

    val artifactId: PathCodec[ArtifactId] =
      PathCodec.string("artifactId").transform(ArtifactId(_))(_.toString)

    val version: PathCodec[Version] =
      PathCodec.string("version").transform(Version(_))(_.toString)

    val groupArtifact: PathCodec[GroupArtifact] =
      val base = groupId / artifactId
      base.transform((g, a) => GroupArtifact(g, a))(ga => (ga.groupId, ga.artifactId))

    val groupArtifactVersion: PathCodec[GroupArtifactVersion] =
      val base = groupId / artifactId / version
      base.transform((g, a, v) => GroupArtifactVersion(g, a, v))(gav => (gav.groupId, gav.artifactId, gav.version))

  object Deploy:
    import zio.http.Header.Authorization

    // would be nice to just type alias but then results in Ambiguous layers
    case class Sonatype(client: Client)

    object Sonatype:
      private val sonatypeUrl = URL.decode("https://central.sonatype.com").toOption.get

      extension (s: String) private def base64: String = java.util.Base64.getEncoder.encodeToString(s.getBytes)

      private def clientMiddleware(username: String, password: String)(client: Client): Client =
        val token = s"$username:$password".base64

        client
          .addHeader(Authorization.Bearer(token = token))
          .url(sonatypeUrl)
          .mapZIO:
            response =>
              if response.status.isSuccess then
                ZIO.succeed(response)
              else
                response.body.asString.flatMap:
                  body =>
                    ZIO.fail(IllegalStateException(s"Sonatype request failed ${response.status} : $body"))

      val Live: ZLayer[Client, Throwable, Sonatype] =
        ZLayer.fromZIO:
          defer:
            val username = ZIO.systemWith(_.env("OSS_DEPLOY_USERNAME")).someOrFail(new RuntimeException("OSS_DEPLOY_USERNAME env var not set")).run
            val password = ZIO.systemWith(_.env("OSS_DEPLOY_PASSWORD")).someOrFail(new RuntimeException("OSS_DEPLOY_PASSWORD env var not set")).run
            val client = ZIO.serviceWith[Client](clientMiddleware(username, password)).run
            Sonatype(client)

      /** Build a Sonatype client from explicit credentials. Use this when
       *  credentials don't come from `OSS_DEPLOY_USERNAME` /
       *  `OSS_DEPLOY_PASSWORD` env vars (e.g. read from typed config). */
      def fromCredentials(username: String, password: String): ZLayer[Client, Nothing, Sonatype] =
        ZLayer.fromFunction((client: Client) => Sonatype(clientMiddleware(username, password)(client)))

    private type DeploymentId = String

    given CanEqual[DeploymentState, DeploymentState] = CanEqual.derived

    enum DeploymentState:
      case FAILED, PENDING, PUBLISHED, PUBLISHING, VALIDATED, VALIDATING
      def isFinal: Boolean = this == FAILED || this == PUBLISHED || this == VALIDATED


    def upload(filename: String, zip: Array[Byte]): ZIO[Sonatype, Throwable, DeploymentId] =
      ZIO.serviceWithZIO[Sonatype]:
        sonatype =>
          ZIO.scoped:
            defer:
              val body = Body.fromMultipartFormUUID(
                Form(
                  FormField.binaryField(
                    "bundle",
                    Chunk.fromArray(zip),
                    MediaType.application.`octet-stream`,
                    filename = Some(filename),
                  )
                )
              ).run
              val response = sonatype.client.post("/api/v1/publisher/upload")(body).run
              response.body.asString.run

    private case class StatusResponse(deploymentState: DeploymentState) derives Schema

    def checkStatus(deploymentId: DeploymentId): ZIO[Sonatype, Throwable, DeploymentState] =
      ZIO.serviceWithZIO[Sonatype]:
        sonatype =>
          ZIO.scoped:
            defer:
              val request = Request.post(s"/api/v1/publisher/status", Body.empty).addQueryParam("id", deploymentId)
              val response = sonatype.client.batched(request).run
              val statusResponse = response.body.asJson[StatusResponse].run
              statusResponse.deploymentState

    private def publish(deploymentId: DeploymentId): ZIO[Sonatype, Throwable, Unit] =
      ZIO.serviceWithZIO[Sonatype]:
        sonatype =>
          ZIO.scoped:
            sonatype.client.post(s"/api/v1/publisher/deployment/$deploymentId")(Body.empty).unit
            // todo: check that the response status is 204

    def drop(deploymentId: DeploymentId): ZIO[Sonatype, Throwable, Unit] =
      ZIO.serviceWithZIO[Sonatype]:
        sonatype =>
          ZIO.scoped:
            sonatype.client.delete(s"/api/v1/publisher/deployment/$deploymentId").unit

    def uploadVerifyAndPublish(filename: String, zip: Array[Byte]): ZIO[Sonatype, Throwable, Unit] =
      ZIO.serviceWithZIO[Sonatype]:
        sonatype =>
          ZIO.scoped:
            defer:
              val deploymentId = MavenCentral.Deploy.upload(filename, zip).run

              val (_, status) = MavenCentral.Deploy.checkStatus(deploymentId)
                .repeat(Schedule.exponential(1.second) && Schedule.recurUntil(_.isFinal))
                .timeout(5.minutes)
                .someOrFail(RuntimeException("Timed out waiting for deployment to finish processing"))
                .run

              if status == DeploymentState.VALIDATED then
                MavenCentral.Deploy.publish(deploymentId).run
              else
                ZIO.fail(RuntimeException(s"Deployment failed with status $status")).run

  trait Signer:
    def ascSign(toSign: Chunk[Byte]): IO[Throwable, Option[Chunk[Byte]]]

  object Signer:
    def make(gpgKey: String, gpgPass: Option[String]): ZLayer[Any, Throwable, Signer] =
      // Parse the key material once; build a fresh, stateful PGPSignatureGenerator
      // per signing call so concurrent callers don't share its internal digest state.
      ZLayer.fromZIO:
        ZIO.attempt:
          val keyBytes = Base64.getDecoder.decode(gpgKey)
          val secretKeyRing = PGPSecretKeyRing(keyBytes, JcaKeyFingerprintCalculator())
          val pass = gpgPass.getOrElse("").toCharArray
          val privKey = secretKeyRing.getSecretKey.extractPrivateKey(JcePBESecretKeyDecryptorBuilder().build(pass))
          val publicKey = secretKeyRing.getPublicKey()
          SignerLive(publicKey, privKey)


  class SignerLive(publicKey: PGPPublicKey, privKey: PGPPrivateKey) extends Signer:

    private def signError(e: Throwable): Throwable =
      RuntimeException(s"Signing failed: ${e.getMessage}", e)

    private final class ChunkBuilderOutputStream(builder: ChunkBuilder[Byte]) extends java.io.OutputStream:
      override def write(b: Int): Unit =
        builder += b.toByte

      override def write(b: Array[Byte], off: Int, len: Int): Unit =
        var i = off
        val end = off + len
        while i < end do
          builder += b(i)
          i += 1

    override def ascSign(toSign: Chunk[Byte]): IO[Throwable, Option[Chunk[Byte]]] =
      // The result chunk MUST be read after the streams are closed:
      // ArmoredOutputStream writes the trailing CRC and end-of-armor
      // marker on close(), and BCPGOutputStream may buffer partial
      // packet data. Reading `builder.result()` before the scope
      // releases produced a truncated, unverifiable signature.
      ZIO.attempt(ChunkBuilder.make[Byte]()).flatMap: builder =>
        ZIO.scoped:
          defer:
            val signerBuilder = JcaPGPContentSignerBuilder(publicKey.getAlgorithm, HashAlgorithmTags.SHA256)
            val sGen = PGPSignatureGenerator(signerBuilder, publicKey)
            ZIO.attempt(sGen.init(PGPSignature.BINARY_DOCUMENT, privKey)).run
            val rawOut = ZIO.fromAutoCloseable(ZIO.attempt(ChunkBuilderOutputStream(builder))).run
            val armor = ZIO.fromAutoCloseable(ZIO.attempt(ArmoredOutputStream(rawOut))).run
            val bOut = ZIO.fromAutoCloseable(ZIO.attempt(BCPGOutputStream(armor))).run
            ZIO.attempt(sGen.update(toSign.toArray)).run
            ZIO.attempt(sGen.generate().encode(bOut)).run
        .as(Some(builder.result()))
      .mapError(signError)
