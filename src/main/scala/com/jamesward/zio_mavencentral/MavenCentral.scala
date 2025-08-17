package com.jamesward.zio_mavencentral

import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import zio.direct.*
import zio.http.{Body, Client, Headers, Method, Path, Request, Response, Status, URL}
import zio.stream.ZPipeline
import zio.{Cause, Scope, Trace, ZIO}

import java.io.File
import java.nio.file.Files
import scala.annotation.{targetName, unused}
import scala.util.matching.Regex

object MavenCentral:

  given CanEqual[Path, Path] = CanEqual.derived
  given CanEqual[GroupId, GroupId] = CanEqual.derived
  given CanEqual[ArtifactId, ArtifactId] = CanEqual.derived
  given CanEqual[Version, Version] = CanEqual.derived
  given CanEqual[GroupArtifact, GroupArtifact] = CanEqual.derived
  given CanEqual[GroupArtifactVersion, GroupArtifactVersion] = CanEqual.derived
  given CanEqual[URL, URL] = CanEqual.derived
  given CanEqual[Status, Status] = CanEqual.derived

  // todo: regionalize to maven central mirrors for lower latency
  private val artifactUri = "https://repo1.maven.org/maven2/"
  private val fallbackArtifactUri = "https://repo.maven.apache.org/maven2/"

  case class GroupIdNotFoundError(groupId: GroupId)
  case class GroupIdOrArtifactIdNotFoundError(groupId: GroupId, artifactId: ArtifactId)
  case class JavadocNotFoundError(groupId: GroupId, artifactId: ArtifactId, version: Version)

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

  private def responseToNames(response: Response): ZIO[Any, Nothing, Seq[String]] =
    response.body.asStream
      .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
      .runFold(Seq.empty[String])(lineExtractor)
      .orDie

  extension (client: Client.type)
    def requestWithFallback(
                                path: Path,
                                method: Method = Method.GET,
                                headers: Headers = Headers.empty,
                                content: Body = Body.empty,
                                primaryBaseUrl: String = artifactUri,
                                fallbackBaseUrl: String = fallbackArtifactUri
                              )(implicit trace: Trace): ZIO[Client & Scope, Nothing, (Response, String)] =
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

      requestAndLog(primaryBaseUrl)
        .orElse(requestAndLog(fallbackBaseUrl))
        .orDie

  def searchArtifacts(groupId: GroupId): ZIO[Client & Scope, GroupIdNotFoundError, Seq[ArtifactId]] =
    defer:
      val path = artifactPath(groupId).addTrailingSlash
      val (response, _) = Client.requestWithFallback(path).run
      response.status match
        case Status.NotFound =>
          ZIO.fail(GroupIdNotFoundError(groupId)).run
        case s if s.isSuccess =>
          responseToNames(response).run.map(ArtifactId(_)).sorted(using CaseInsensitiveOrdering)
        case _ =>
          val body = response.body.asString.orDie.run
          ZIO.dieMessage(body).run


  def searchVersions(groupId: GroupId, artifactId: ArtifactId): ZIO[Client & Scope, GroupIdOrArtifactIdNotFoundError, Seq[Version]] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId))).addTrailingSlash
      val (response, _) = Client.requestWithFallback(path).run
      response.status match
        case Status.NotFound =>
          ZIO.fail(GroupIdOrArtifactIdNotFoundError(groupId, artifactId)).run
        case s if s.isSuccess =>
          responseToNames(response).run
            .sortBy(semverfi.Version(_))
            .reverse
            .map(Version(_))
        case _ =>
          val body = response.body.asString.orDie.run
          ZIO.dieMessage(body).run

  def latest(groupId: GroupId, artifactId: ArtifactId): ZIO[Client & Scope, GroupIdOrArtifactIdNotFoundError, Option[Version]] =
    searchVersions(groupId, artifactId).map(_.headOption)

  def isArtifact(groupId: GroupId, artifactId: ArtifactId): ZIO[Client & Scope, Nothing, Boolean] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId))) / "maven-metadata.xml"
      val (response, _) = Client.requestWithFallback(path).run
      response.status match
        case s if s.isSuccess =>
          val body = response.body.asString.orDie.run
          // checks that the maven-metadata.xml contains the groupId
          body.contains(s"<groupId>$groupId</groupId>")
        case _ =>
          false

  def artifactExists(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client & Scope, Nothing, Boolean] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))).addTrailingSlash
      val (response, _) = Client.requestWithFallback(path, Method.HEAD).run
      response.status.isSuccess

  def javadocUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[Client & Scope, JavadocNotFoundError, URL] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version-javadoc.jar"
      val (response, url) = Client.requestWithFallback(path, Method.HEAD).run
      response.status match
        case status if status.isSuccess =>
          ZIO.fromEither(URL.decode(url)).orDie.run
        case Status.NotFound =>
          ZIO.fail(JavadocNotFoundError(groupId, artifactId, version)).run
        case _ =>
          val body = response.body.asString.orDie.run
          ZIO.dieMessage(body).run

  // todo: this is terrible
  //       zip handling via zio?
  //       what about file locking
  def downloadAndExtractZip(source: URL, destination: File): ZIO[Client & Scope, Nothing, Unit] =
    defer:
      val request = Request.get(source)
      val response = Client.batched(request).orDie.run
      if response.status.isError then {
        val body = response.body.asString.orDie.run
        ZIO.dieMessage(body).run
      } else
        run:
          ZIO.scoped:
            defer:
              val zipArchiveInputStream =
                run:
                  ZIO.fromAutoCloseable:
                    response.body.asStream.orDie.toInputStream.map(ZipArchiveInputStream(_))

              LazyList
                .continually(zipArchiveInputStream.getNextEntry)
                .takeWhile:
                  case _: ArchiveEntry => true
                  case _ => false

                .foreach: ze =>
                  val tmpFile = File(destination, ze.nn.getName)
                  if (ze.nn.isDirectory)
                    tmpFile.mkdirs()
                  else
                    if (!tmpFile.getParentFile.nn.exists())
                      tmpFile.getParentFile.nn.mkdirs()
                    Files.copy(zipArchiveInputStream, tmpFile.toPath)
