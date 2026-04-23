package com.jamesward.zio_mavencentral

import zio.direct.*
import zio.http.*
import zio.schema.{Schema, derived}
import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Cause, Chunk, Schedule, Scope, Trace, ZIO, ZLayer, durationInt}

import java.io.{File, IOException}
import java.nio.file.Files
import java.time.ZonedDateTime
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
    // todo: more defensive
    def asGroupArtifact: GroupArtifact =
      val parts = groupId.split('.')
      GroupArtifact(GroupId(parts.init.mkString(".")), ArtifactId(parts.last))

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

  case class GroupIdNotFoundError(groupId: GroupId)
  case class GroupIdOrArtifactIdNotFoundError(groupId: GroupId, artifactId: ArtifactId)
  case class NotFoundError(groupId: GroupId, artifactId: ArtifactId, version: Version)
  case class UnknownError(response: Response) extends Throwable(response.status.text)
  case class ParseError(t: Throwable) extends Throwable(t)

  given Schema[GroupId] = Schema.primitive[String].transform(MavenCentral.GroupId.apply, _.toString)
  given Schema[ArtifactId] = Schema.primitive[String].transform(MavenCentral.ArtifactId.apply, _.toString)
  given Schema[Version] = Schema.primitive[String].transform(MavenCentral.Version.apply, _.toString)

  def stringToGroupArtifact(s: String): MavenCentral.GroupArtifact =
    // todo: parse failure handling
    val parts = s.split(':')
    MavenCentral.GroupArtifact(MavenCentral.GroupId(parts(0)), MavenCentral.ArtifactId(parts(1)))

  def groupArtifactToString(ga: MavenCentral.GroupArtifact): String = s"${ga.groupId}:${ga.artifactId}"

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

  // Removes any versions found in this groupId
  def searchArtifacts(groupId: GroupId): ZIO[Client, GroupIdNotFoundError | Throwable, WithCacheInfo[Seq[ArtifactId]]] =
    val path = artifactPath(groupId).addTrailingSlash
    val allFilesReq = Client.requestWithFallback(path).map(_._1)
    val groupArtifact = groupId.asGroupArtifact
    val versions = searchVersions(groupArtifact.groupId, groupArtifact.artifactId).map(_.value).orElseSucceed(Seq.empty[Version])
    allFilesReq.zipWithPar(versions):
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
            ZIO.fail(UnknownError(allFilesResp)).run
    .flatten


  // newest first
  def searchVersions(groupId: GroupId, artifactId: ArtifactId): ZIO[Client, GroupIdOrArtifactIdNotFoundError | Throwable, WithCacheInfo[Seq[Version]]] =
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
          case e => ZIO.fail(UnknownError(response)).run

  // todo: potentially use maven-metadata.xml
  def latest(groupId: GroupId, artifactId: ArtifactId): ZIO[Client, GroupIdOrArtifactIdNotFoundError | Throwable, Option[Version]] =
    searchVersions(groupId, artifactId).map(_.value.headOption)

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

  def pom(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, NotFoundError | Throwable, Elem] =
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
          ZIO.fail(UnknownError(response)).run

  def mavenMetadata(groupId: GroupId, artifactId: ArtifactId): ZIO[Client, GroupIdOrArtifactIdNotFoundError | Throwable, WithCacheInfo[Elem]] =
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
          ZIO.fail(UnknownError(response)).run

  def javadocUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, NotFoundError | Throwable, URL] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version-javadoc.jar"
      val (response, url) = Client.requestWithFallback(path, Method.HEAD).run
      response.status match
        case status if status.isSuccess =>
          ZIO.succeed(url).run
        case Status.NotFound =>
          ZIO.fail(NotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(UnknownError(response)).run

  def sourcesUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client, NotFoundError | Throwable, URL] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version-sources.jar"
      val (response, url) = Client.requestWithFallback(path, Method.HEAD).run
      response.status match
        case status if status.isSuccess =>
          ZIO.succeed(url).run
        case Status.NotFound =>
          ZIO.fail(NotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(UnknownError(response)).run

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

  // Download a zip to a temporary file, extract it to `destination`, then delete the tmp file.
  // The download step returns HTTP response headers for cache metadata extraction.
  private def downloadAndExtract(source: URL, destination: File): ZIO[Client, Throwable, (Response, Set[String])] =
    ZIO.scoped:
      defer:
        val tmpFile = ZIO.acquireRelease(
          ZIO.attemptBlockingIO(Files.createTempFile("mvncentral-dl-", ".zip").nn)
        )(path => ZIO.attemptBlockingIO(Files.deleteIfExists(path)).ignoreLogged).run
        val response = Client.streaming(Request.get(source)).run
        if response.status.isError then
          ZIO.fail(UnknownError(response)).run
        response.body.asStream.run(ZSink.fromPath(tmpFile)).run
        val files = extractZipFile(tmpFile, destination).run
        (response, files)

  // Download and extract a zip, returning the list of extracted files along with
  // Maven Central cache metadata (Last-Modified / ETag) from the HTTP response.
  def downloadAndExtractZip(source: URL, destination: File): ZIO[Client, Throwable, WithCacheInfo[Set[String]]] =
    downloadAndExtract(source, destination).map: (response, files) =>
      WithCacheInfo(
        files,
        response.header(Header.LastModified).map(_.value),
        response.header(Header.ETag),
      )

  object Deploy:
    import zio.http.Header.Authorization

    // would be nice to just type alias but then results in Ambiguous layers
    case class Sonatype(client: Client)

    object Sonatype:
      private val sonatypeUrl = URL.decode("https://central.sonatype.com").toOption.get

      extension (s: String) def base64: String = java.util.Base64.getEncoder.encodeToString(s.getBytes)

      def clientMiddleware(username: String, password: String)(client: Client): Client =
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

    type DeploymentId = String

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

    case class StatusResponse(deploymentState: DeploymentState) derives Schema

    def checkStatus(deploymentId: DeploymentId): ZIO[Sonatype, Throwable, DeploymentState] =
      ZIO.serviceWithZIO[Sonatype]:
        sonatype =>
          ZIO.scoped:
            defer:
              val request = Request.post(s"/api/v1/publisher/status", Body.empty).addQueryParam("id", deploymentId)
              val response = sonatype.client.request(request).run
              val statusResponse = response.body.asJson[StatusResponse].run
              statusResponse.deploymentState

    def publish(deploymentId: DeploymentId): ZIO[Sonatype, Throwable, Unit] =
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
