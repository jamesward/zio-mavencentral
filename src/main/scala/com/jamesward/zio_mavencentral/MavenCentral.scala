package com.jamesward.zio_mavencentral

import zio.compress.{ArchiveEntry, ZipUnarchiver}
import zio.direct.*
import zio.http.*
import zio.schema.{Schema, derived}
import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Cause, Chunk, Schedule, Scope, Trace, ZIO, ZLayer, durationInt}

import java.io.{File, IOException}
import java.nio.file.Files
import java.time.ZonedDateTime
import java.util.zip.ZipEntry
import scala.annotation.{targetName, unused}
import scala.util.matching.Regex
import scala.xml.Elem

object MavenCentral:

  // todo: regionalize to maven central mirrors for lower latency
  private val artifactUri = "https://repo1.maven.org/maven2/"
  private val fallbackArtifactUri = "https://repo.maven.apache.org/maven2/"


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

  case class SeqWithLastModified[A](items: Seq[A], maybeLastModified: Option[ZonedDateTime])

  case class GroupIdNotFoundError(groupId: GroupId)
  case class GroupIdOrArtifactIdNotFoundError(groupId: GroupId, artifactId: ArtifactId)
  case class JavadocNotFoundError(groupId: GroupId, artifactId: ArtifactId, version: Version)
  case class UnknownError(response: Response) extends Throwable
  case class ParseError(t: Throwable) extends Throwable

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
                                primaryBaseUrl: String = artifactUri,
                                fallbackBaseUrl: String = fallbackArtifactUri
                              )(implicit trace: Trace): ZIO[Client & Scope, Throwable, (Response, String)] =
      def requestAndLog(baseUrl: String) =
        val url = baseUrl + path
        defer:
          ZIO.log(s"$method $url").run
          val requestUrl = ZIO.fromEither(zio.http.URL.decode(url)).run
          val request = Request(zio.http.Version.Default, method, requestUrl, headers, content)
          client.batched(request)
            .tap:
              response =>
                ZIO.log(s"$method $url ${response.status}")
            .map:
              _ -> url
            .run

      defer:
        requestAndLog(primaryBaseUrl)
          .catchSomeCause:
            case cause: Cause[Throwable] =>
              defer:
                ZIO.logCause(cause).run
                requestAndLog(fallbackBaseUrl).run
          .run

  def searchArtifacts(groupId: GroupId): ZIO[Client & Scope, GroupIdNotFoundError | Throwable, SeqWithLastModified[ArtifactId]] =
    defer:
      val path = artifactPath(groupId).addTrailingSlash
      val (response, _) = Client.requestWithFallback(path).run
      response.status match
        case Status.NotFound =>
          ZIO.fail(GroupIdNotFoundError(groupId)).run
        case s if s.isSuccess =>
          SeqWithLastModified(
            responseToNames(response).run.map(ArtifactId(_)).sorted(using CaseInsensitiveOrdering),
            response.header(Header.LastModified).map(_.value)
          )
        case _ =>
          ZIO.fail(UnknownError(response)).run

  def searchVersions(groupId: GroupId, artifactId: ArtifactId): ZIO[Client & Scope, GroupIdOrArtifactIdNotFoundError | Throwable, SeqWithLastModified[Version]] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId))).addTrailingSlash
      val (response, _) = Client.requestWithFallback(path).run
      response.status match
        case Status.NotFound =>
          ZIO.fail(GroupIdOrArtifactIdNotFoundError(groupId, artifactId)).run
        case s if s.isSuccess =>
          SeqWithLastModified(
            responseToNames(response).run
              .sortBy(semverfi.Version(_))
              .reverse
              .map(Version(_)),
            response.header(Header.LastModified).map(_.value)
          )
        case _ =>
          ZIO.fail(UnknownError(response)).run

  def isModifiedSince(dateTime: ZonedDateTime, groupId: GroupId, maybeArtifactId: Option[ArtifactId] = None):
    ZIO[Client & Scope, GroupIdNotFoundError | GroupIdOrArtifactIdNotFoundError | Throwable, Boolean] =
      defer:
        val path = artifactPath(groupId, maybeArtifactId.map(ArtifactAndVersion(_))).addTrailingSlash
        val (response, _) = Client.requestWithFallback(path, Method.HEAD, Headers(Header.IfModifiedSince(dateTime))).run
        response.status match
          case Status.Ok => ZIO.succeed(true).run
          case Status.NotModified => ZIO.succeed(false).run
          case Status.NotFound => ZIO.fail(maybeArtifactId.fold(GroupIdNotFoundError(groupId))(GroupIdOrArtifactIdNotFoundError(groupId, _))).run
          case e => ZIO.fail(UnknownError(response)).run

  def latest(groupId: GroupId, artifactId: ArtifactId): ZIO[Client & Scope, GroupIdOrArtifactIdNotFoundError | Throwable, Option[Version]] =
    searchVersions(groupId, artifactId).map(_.items.headOption)

  def isArtifact(groupId: GroupId, artifactId: ArtifactId): ZIO[Client & Scope, Throwable, Boolean] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId))) / "maven-metadata.xml"
      val (response, _) = Client.requestWithFallback(path).run
      response.status match
        case s if s.isSuccess =>
          val body = response.body.asString.run
          // checks that the maven-metadata.xml contains the groupId
          body.contains(s"<groupId>$groupId</groupId>")
        case _ =>
          false

  def artifactExists(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client & Scope, Throwable, Boolean] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))).addTrailingSlash
      val (response, _) = Client.requestWithFallback(path, Method.HEAD).run
      response.status.isSuccess

  def pom(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client & Scope, JavadocNotFoundError | Throwable, Elem] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version.pom"
      val (response, url) = Client.requestWithFallback(path, Method.GET).run
      response.status match
        case status if status.isSuccess =>
          val body = response.body.asString.run
          scala.xml.XML.loadString(body)
        case Status.NotFound =>
          ZIO.fail(JavadocNotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(UnknownError(response)).run

  def javadocUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client & Scope, JavadocNotFoundError | Throwable, URL] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version-javadoc.jar"
      val (response, url) = Client.requestWithFallback(path, Method.HEAD).run
      response.status match
        case status if status.isSuccess =>
          ZIO.fromEither(URL.decode(url)).run
        case Status.NotFound =>
          ZIO.fail(JavadocNotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(UnknownError(response)).run

  // what about file locking?
  def downloadAndExtractZip(source: URL, destination: File): ZIO[Client & Scope, Throwable, Unit] =
    defer:
      val request = Request.get(source)
      val response = Client.batched(request).run
      if response.status.isError then
        ZIO.fail(UnknownError(response)).run
      else
        val sink = ZSink.foreach[Any, Throwable, (ArchiveEntry[Option, ZipEntry], ZStream[Any, IOException, Byte])]:
          case (entry, contentStream) =>
            val targetPath = destination.toPath.resolve(entry.name)

            if entry.isDirectory then
              ZIO.attemptBlockingIO(Files.createDirectories(targetPath))
            else
              ZIO.attemptBlockingIO(Files.createDirectories(targetPath.getParent)) *>
                contentStream.run(ZSink.fromPath(targetPath))

        response.body.asStream.via(ZipUnarchiver.unarchive).run(sink).unit.run

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
        @@ ZClientAspect.requestLogging()

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
              val response = sonatype.client.post(s"/api/v1/publisher/status?id=$deploymentId")(Body.empty).run
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

              val status = MavenCentral.Deploy.checkStatus(deploymentId)
                .filterOrFail(_.isFinal)(IllegalStateException("Waiting on final deployment status")) // todo: add current state to error
                .retry(Schedule.exponential(1.second))
                .timeout(5.minutes)
                .someOrFail(RuntimeException("Timed out waiting for deployment to finish processing"))
                .run

              if status == DeploymentState.VALIDATED then
                MavenCentral.Deploy.publish(deploymentId).run
              else
                ZIO.fail(RuntimeException(s"Deployment failed with status $status")).run
