package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.*
import zio.test.*

/** Version ordering and pre-release detection; pure, no network. */
object VersionSpec extends ZIOSpecDefault:

  private def vs(s: String*): Seq[Version] = s.map(Version(_))
  private def str(v: Seq[Version]): Seq[String] = v.map(_.toString)

  def spec = suite("Version")(
    test("Maven ordering: numeric segments, and qualifiers below the release"):
      assertTrue(
        str(newestFirst(vs("2.9.0", "2.10.0", "2.0.0-RC2", "2.0.0", "2.0.0-M8", "2.0.1", "2.1.0-M1"))) ==
          Seq("2.10.0", "2.9.0", "2.1.0-M1", "2.0.1", "2.0.0", "2.0.0-RC2", "2.0.0-M8"),
        str(newestFirst(vs("1.0-SNAPSHOT", "1.0", "1.0-alpha-1", "1.0-beta-1", "1.0-rc-1"))) ==
          Seq("1.0", "1.0-SNAPSHOT", "1.0-rc-1", "1.0-beta-1", "1.0-alpha-1"),
      )
    ,
    test("publish order is not version order: a late maintenance release is not the latest"):
      // jackson-databind's maven-metadata.xml really lists 2.4.1.3 after 2.4.2
      val publishOrder = vs("2.4.1", "2.4.2", "2.4.1.3")
      assertTrue(
        latestOf(publishOrder).contains(Version("2.4.2")),
        str(newestFirst(publishOrder)) == Seq("2.4.2", "2.4.1.3", "2.4.1"),
      )
    ,
    test("pre-release markers"):
      val pre = Seq("2.1.0-M1", "5.0.0.Alpha2", "8.0.0.Beta3", "3.0.0-beta3", "2.1.0-alpha1", "4.0.0-rc-7",
        "1.0.0-rc-1036", "3.10.0-RC3", "2.5.0-Beta1", "1.0-SNAPSHOT", "1.9.0-dev-123", "3.0.0-preview1",
        "2.0.0.M1", "1.0-b01", "2.1.0-eap-2", "21-ea", "1.0.0-milestone-2", "4.1.0-CR1")
      val release = Seq("2.0.1", "33.7.2-jre", "33.7.2-android", "4.2.18.Final", "2.4.1.3", "1.12.797",
        "5.0.0.RELEASE", "2.0.0-GA", "3.2.3", "1.0.0-jdk17", "2.7.0-SP1", "1.0-armv7")
      assertTrue(
        pre.forall(v => Version.isPreRelease(Version(v))),
        release.forall(v => !Version.isPreRelease(Version(v))),
      )
    ,
    test("latest excludes pre-releases unless asked, and falls back when there is no release"):
      val springAi = vs("2.0.0-M8", "2.0.0-RC2", "2.0.0", "2.0.1", "2.1.0-M1")
      val netty = vs("4.2.17.Final", "4.2.18.Final", "5.0.0.Alpha1", "5.0.0.Alpha2")
      val onlyPre = vs("0.1.0-alpha1", "0.1.0-alpha2")
      assertTrue(
        latestOf(springAi).contains(Version("2.0.1")),
        latestOf(springAi, includePreReleases = true).contains(Version("2.1.0-M1")),
        latestOf(netty).contains(Version("4.2.18.Final")),
        latestOf(netty, includePreReleases = true).contains(Version("5.0.0.Alpha2")),
        latestOf(onlyPre).contains(Version("0.1.0-alpha2")),
        latestOf(Seq.empty).isEmpty,
      )
  )
