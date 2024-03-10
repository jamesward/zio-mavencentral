package com.jamesward.zio_mavencentral

import MavenCentral.{*, given}
import zio.*
import zio.direct.*
import zio.http.{Client, Path, URL}
import zio.test.*
import zio.test.Assertion.isTrue

import java.nio.file.Files

object MavenCentralSpec extends ZIOSpecDefault:

  given CanEqual[String, String] = CanEqual.derived
  given CanEqual[Seq[MavenCentral.ArtifactId], Seq[MavenCentral.ArtifactId]] = CanEqual.derived
  given CanEqual[Exit[MavenCentral.JavadocNotFoundError | Throwable, ?], Exit[MavenCentral.JavadocNotFoundError | Throwable, ?]] = CanEqual.derived

  def spec = suite("MavenCentral")(
    test("artifactPath"):
      assertTrue(
        artifactPath(GroupId("org.webjars")) == Path.decode("org/webjars"),
        artifactPath(GroupId("org.webjars"), Some(ArtifactAndVersion(ArtifactId("jquery")))) == Path.decode("org/webjars/jquery"),
        artifactPath(GroupId("org.webjars"), Some(ArtifactAndVersion(ArtifactId("jquery"), Some(Version("3.6.4"))))) == Path.decode("org/webjars/jquery/3.6.4")
      )
    ,
    test("searchArtifacts"):
      defer:
        val webjarArtifacts = searchArtifacts(GroupId("org.webjars")).run
        val springdataArtifacts = searchArtifacts(GroupId("org.springframework.data")).run
        val err = searchArtifacts(GroupId("zxcv12313asdf")).flip.run

        assertTrue(
          webjarArtifacts.size > 1000,
          webjarArtifacts == webjarArtifacts.sorted(CaseInsensitiveOrdering),
          springdataArtifacts.size > 10,
          err.isInstanceOf[GroupIdNotFoundError],
        )
    ,
    test("searchVersions"):
      defer:
        val versions = searchVersions(GroupId("org.webjars"), ArtifactId("jquery")).run
        val err = searchVersions(GroupId("com.jamesward"), ArtifactId("zxcvasdf")).flip.run

        assertTrue(
          versions.contains("3.6.4"),
          versions.indexOf(Version("1.12.4")) < versions.indexOf(Version("1.5.2")),
          err.isInstanceOf[GroupIdOrArtifactIdNotFoundError],
        )
    ,
    test("searchVersions does not change versions"):
      defer:
        val versions = searchVersions(GroupId("io.jenkins.archetypes"), ArtifactId("archetypes-parent")).run
        assertTrue:
          versions.contains("1.21")
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
          doesNotExist == Exit.fail(JavadocNotFoundError(GroupId("com.jamesward"), ArtifactId("travis-central-test"), Version("0.0.15"))),
          URL.decode("https://repo1.maven.org/maven2/org/webjars/webjars-locator-core/0.52/webjars-locator-core-0.52-javadoc.jar").contains(doesExist),
        )
    ,
    test("downloadAndExtractZip"):
      val url = URL.decode("https://repo1.maven.org/maven2/com/jamesward/travis-central-test/0.0.15/travis-central-test-0.0.15.jar").toOption.get
      val tmpFile = Files.createTempDirectory("test").nn.toFile.nn
      downloadAndExtractZip(url, tmpFile).as(assertTrue(tmpFile.list().nn.contains("META-INF")))
    ,
    // note that on some networks all DNS requests are accepted and redirect to something like a captive portal, wtf
    test("requestWithFallbackurl"):
      val artifactUrl = "https://zxcvasdf123124zxcv.com/"
      val fallbackArtifactUrl = "https://repo1.maven.org/maven2/"
      // bug in zio-direct:
      // assertTrue(response.status.isSuccess)
      // Exception occurred while executing macro expansion.
      // java.lang.Exception: Expected an expression. This is a partially applied Term. Try eta-expanding the term first.
      Client.requestWithFallback(Path.decode("com/jamesward/maven-metadata.xml"), primaryBaseUrl = artifactUrl, fallbackBaseUrl = fallbackArtifactUrl).map:
        (response, _) =>
          assert(response.status.isSuccess)(isTrue)
  ).provide(Client.default, Scope.default)
