package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.{GroupArtifactVersion, JarMeta, NotFoundError, gav}
import zio.*
import zio.direct.*
import zio.http.Client
import zio.test.*

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipOutputStream}

object JarCacheSpec extends ZIOSpecDefault:

  /** Build a real on-disk jar with the given (path -> bytes) entries. */
  private def writeJar(target: File, entries: Map[String, Array[Byte]]): Unit =
    target.getParentFile.mkdirs()
    val out = new ZipOutputStream(java.io.FileOutputStream(target))
    try
      for (path, bytes) <- entries do
        out.putNextEntry(new ZipEntry(path))
        out.write(bytes)
        out.closeEntry()
    finally out.close()

  /** Recursively delete a directory tree. Safe on missing paths. */
  private def recursivelyDelete(f: File): Unit =
    if f.isDirectory then
      val cs = f.listFiles
      if cs != null then cs.foreach(recursivelyDelete)
    f.delete()
    ()

  /** Test downloader factory: each call to `download(gav, target)` writes
   *  the corresponding fixture jar (built lazily from `entriesFor`) to
   *  `target`. Optionally awaits a gate for concurrency tests, and
   *  records call counts per GAV. */
  private def fakeDownloader(
    entriesFor: GroupArtifactVersion => Option[Map[String, Array[Byte]]],
    callCount: Ref[Map[GroupArtifactVersion, Int]],
    gate: Option[Promise[Nothing, Unit]] = None,
  ): (GroupArtifactVersion, File) => ZIO[Client, NotFoundError, JarMeta] =
    (gav, target) =>
      defer:
        callCount.update(m => m.updated(gav, m.getOrElse(gav, 0) + 1)).run
        ZIO.foreachDiscard(gate)(_.await).run
        entriesFor(gav) match
          case None =>
            ZIO.fail(NotFoundError(gav.groupId, gav.artifactId, gav.version)).run
          case Some(entries) =>
            ZIO.attemptBlockingIO(writeJar(target, entries)).orDie.run
            JarMeta(None, None)

  private val gavA = gav("g", "a", "1")
  private val gavB = gav("g", "b", "1")
  private val gavMissing = gav("g", "missing", "1")

  private val sampleEntries: Map[String, Array[Byte]] = Map(
    "index.html"        -> "<html>index</html>".getBytes(StandardCharsets.UTF_8),
    "pkg/Foo.html"      -> "<html>Foo</html>".getBytes(StandardCharsets.UTF_8),
    "pkg/Bar.html"      -> "<html>Bar</html>".getBytes(StandardCharsets.UTF_8),
    "pkg/sub/Baz.html"  -> "<html>Baz</html>".getBytes(StandardCharsets.UTF_8),
    "element-list"      -> "pkg\npkg.sub\n".getBytes(StandardCharsets.UTF_8),
  )

  private val fixtures: GroupArtifactVersion => Option[Map[String, Array[Byte]]] =
    g => if g == gavMissing then None else Some(sampleEntries)

  /** Provides a fresh JarCache (rooted in a unique tmp dir) plus the
   *  call-count Ref to the test body. The cache's scope cleans up
   *  ZipFile handles; we also recursively delete the tmp dir. */
  private def withCache[E, A](
    body: (JarCache, Ref[Map[GroupArtifactVersion, Int]]) => ZIO[Client & MavenCentral.MavenCentralRepo & Scope, E, A],
    gate: Option[Promise[Nothing, Unit]] = None,
  ): ZIO[Client & MavenCentral.MavenCentralRepo & Scope, E, A] =
    defer:
      val callCount = Ref.make(Map.empty[GroupArtifactVersion, Int]).run
      val tmp = ZIO.attemptBlockingIO(Files.createTempDirectory("jar-cache-test").nn.toFile).orDie.run
      ZIO.addFinalizer(ZIO.attempt(recursivelyDelete(tmp)).ignore).run
      val cache = JarCache.make(tmp, fakeDownloader(fixtures, callCount, gate)).run
      body(cache, callCount).run

  def spec = suite("JarCache")(

    test("get downloads, opens, and reads entries via random access") {
      val body = ZIO.scoped:
        withCache: (cache, _) =>
          defer:
            val handle    = cache.get(gavA).run
            val hasFoo    = handle.hasEntry("pkg/Foo.html").run
            val hasNope   = handle.hasEntry("nope").run
            val fooString = handle.readEntryString("pkg/Foo.html").run
            assertTrue(
              handle.gav == gavA,
              hasFoo,
              !hasNope,
              fooString == "<html>Foo</html>",
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("entryNames lists all entries; filterEntryNames matches predicate") {
      val body = ZIO.scoped:
        withCache: (cache, _) =>
          defer:
            val handle = cache.get(gavA).run
            val all    = handle.entryNames.run
            val htmls  = handle.filterEntryNames(_.endsWith(".html")).run
            assertTrue(
              all == sampleEntries.keySet,
              htmls == Set("index.html", "pkg/Foo.html", "pkg/Bar.html", "pkg/sub/Baz.html"),
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("readEntry on a missing path fails with JarEntryNotFound") {
      val body = ZIO.scoped:
        withCache: (cache, _) =>
          defer:
            val handle = cache.get(gavA).run
            val result = handle.readEntry("does-not-exist.html").either.run
            val expected = JarCache.JarEntryNotFound(gavA, "does-not-exist.html")
            assertTrue(result.left.toOption.contains(expected))
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("repeated get for the same GAV downloads only once") {
      val body = ZIO.scoped:
        withCache: (cache, callCount) =>
          defer:
            val h1 = cache.get(gavA).run
            val h2 = cache.get(gavA).run
            val h3 = cache.get(gavA).run
            val n  = callCount.get.map(_.getOrElse(gavA, 0)).run
            assertTrue(
              n == 1,
              // Same handle instance returned on hit.
              (h1 eq h2) && (h2 eq h3),
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("concurrent get for the same GAV deduplicates downloads") {
      val body = ZIO.scoped:
        defer:
          val gate = Promise.make[Nothing, Unit].run
          withCache(
            (cache, callCount) =>
              defer:
                val fibers = ZIO.foreachPar(1 to 16): _ =>
                  cache.get(gavA)
                .fork.run
                // All 16 fibers contend on the in-flight download. Open
                // the gate so the single owner can finish.
                gate.succeed(()).run
                val handles = fibers.join.run
                val n = callCount.get.map(_.getOrElse(gavA, 0)).run
                assertTrue(
                  n == 1,
                  handles.size == 16,
                  handles.forall(_ eq handles.head),
                ),
            Some(gate),
          ).run
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("get for a missing GAV fails with NotFoundError and is not cached") {
      val body = ZIO.scoped:
        withCache: (cache, callCount) =>
          defer:
            val r1 = cache.get(gavMissing).either.run
            val r2 = cache.get(gavMissing).either.run
            val n  = callCount.get.map(_.getOrElse(gavMissing, 0)).run
            val expected = NotFoundError(gavMissing.groupId, gavMissing.artifactId, gavMissing.version)
            assertTrue(
              r1.left.toOption.contains(expected),
              r2.left.toOption.contains(expected),
              // A failure is not cached: a retry re-invokes the downloader.
              n == 2,
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("size and totalBytes reflect cached entries") {
      val body = ZIO.scoped:
        withCache: (cache, _) =>
          defer:
            val sizeEmpty = cache.size.run
            val bytesEmpty = cache.totalBytes.run
            cache.get(gavA).run
            cache.get(gavB).run
            val sizeAfter = cache.size.run
            val bytesAfter = cache.totalBytes.run
            assertTrue(
              sizeEmpty == 0,
              bytesEmpty == 0L,
              sizeAfter == 2,
              bytesAfter > 0L,
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("contains returns true only after a successful get") {
      val body = ZIO.scoped:
        withCache: (cache, _) =>
          defer:
            val before = cache.contains(gavA).run
            cache.get(gavA).run
            val after = cache.contains(gavA).run
            assertTrue(!before, after)
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("ZipFile handles are closed when the cache scope finalizes") {
      // Open a cache, fetch a jar, then close the scope. After that
      // reading via the captured handle must fail because its ZipFile
      // was closed.
      val body = ZIO.scoped:
        defer:
          val callCount = Ref.make(Map.empty[GroupArtifactVersion, Int]).run
          val tmp = ZIO.attemptBlockingIO(Files.createTempDirectory("jar-cache-close-test").nn.toFile).orDie.run
          ZIO.addFinalizer(ZIO.attempt(recursivelyDelete(tmp)).ignore).run

          val handle = ZIO.scoped:
            defer:
              val cache = JarCache.make(tmp, fakeDownloader(fixtures, callCount)).run
              cache.get(gavA).run
          .run

          val readResult = handle.readEntry("pkg/Foo.html").exit.run
          assertTrue(readResult.isFailure)
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

  ) @@ TestAspect.withLiveClock
