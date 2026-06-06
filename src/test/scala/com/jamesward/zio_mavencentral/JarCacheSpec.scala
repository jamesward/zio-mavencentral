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

  /** Variant of [[withCache]] with explicit warm-idle and sweep-interval
   *  durations, for tests that exercise the warm/cold sweeper. The
   *  defaults in `JarCache.make` are tuned for production (30 min /
   *  1 min); tests need much smaller values. */
  private def withTunedCache[E, A](
    warmIdleTtl: Duration,
    sweepInterval: Duration,
    coldIdleTtl: Option[Duration] = None,
  )(
    body: (JarCache, Ref[Map[GroupArtifactVersion, Int]]) => ZIO[Client & MavenCentral.MavenCentralRepo & Scope, E, A],
  ): ZIO[Client & MavenCentral.MavenCentralRepo & Scope, E, A] =
    defer:
      val callCount = Ref.make(Map.empty[GroupArtifactVersion, Int]).run
      val tmp = ZIO.attemptBlockingIO(Files.createTempDirectory("jar-cache-test").nn.toFile).orDie.run
      ZIO.addFinalizer(ZIO.attempt(recursivelyDelete(tmp)).ignore).run
      val cache = JarCache.make(
        tmp,
        fakeDownloader(fixtures, callCount),
        warmIdleTtl   = warmIdleTtl,
        coldIdleTtl   = coldIdleTtl,
        sweepInterval = sweepInterval,
      ).run
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

    test("streamEntry reads bytes without materializing the whole entry") {
      val body = ZIO.scoped:
        withCache: (cache, _) =>
          defer:
            val handle = cache.get(gavA).run
            // Use a tiny chunkSize to make sure the result is reassembled
            // from multiple chunks rather than a single read.
            val asString =
              handle
                .streamEntry("pkg/Foo.html", chunkSize = 4)
                .via(zio.stream.ZPipeline.utf8Decode)
                .runFold("")(_ + _)
                .run
            assertTrue(asString == "<html>Foo</html>")
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("streamEntry on a missing path fails with JarEntryNotFound") {
      val body = ZIO.scoped:
        withCache: (cache, _) =>
          defer:
            val handle = cache.get(gavA).run
            val result = handle.streamEntry("does-not-exist.html").runCollect.either.run
            val expected = JarCache.JarEntryNotFound(gavA, "does-not-exist.html")
            assertTrue(result.left.toOption.contains(expected))
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("repeated get for the same GAV downloads only once") {
      val body = ZIO.scoped:
        withCache: (cache, callCount) =>
          defer:
            cache.get(gavA).run
            cache.get(gavA).run
            cache.get(gavA).run
            val n      = callCount.get.map(_.getOrElse(gavA, 0)).run
            val sized  = cache.size.run
            assertTrue(
              n == 1,
              // Cache has a single entry; the handles returned above all
              // delegate to the same `CacheEntry` (handles themselves are
              // lightweight pointers and are no longer reference-equal).
              sized == 1,
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
                val sized = cache.size.run
                assertTrue(
                  n == 1,
                  handles.size == 16,
                  // All callers see the single cached entry.
                  sized == 1,
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

    test("warmCount tracks resident ZipFile handles separately from size") {
      // After a fresh fetch, the entry is warm (open ZipFile resident).
      val body = ZIO.scoped:
        withTunedCache(warmIdleTtl = 1.hour, sweepInterval = 1.hour): (cache, _) =>
          defer:
            cache.get(gavA).run
            cache.get(gavB).run
            val s    = cache.size.run
            val warm = cache.warmCount.run
            assertTrue(s == 2, warm == 2)
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("entries demote to cold after warmIdleTtl elapses with no access") {
      // Tiny idle TTL + tight sweep so the test doesn't have to wait long.
      // After fetching a jar and then idling for > warmIdleTtl, the
      // sweeper should close its ZipFile (warmCount → 0) but keep the
      // on-disk file (size unchanged).
      val body = ZIO.scoped:
        withTunedCache(warmIdleTtl = 100.millis, sweepInterval = 50.millis): (cache, _) =>
          defer:
            cache.get(gavA).run
            // Wait long enough for at least one sweep after the TTL.
            ZIO.sleep(400.millis).run
            val s    = cache.size.run
            val warm = cache.warmCount.run
            assertTrue(s == 1, warm == 0)
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("a cold entry re-promotes to warm on next access without re-downloading") {
      val body = ZIO.scoped:
        withTunedCache(warmIdleTtl = 100.millis, sweepInterval = 50.millis): (cache, callCount) =>
          defer:
            // Warm the entry, then let it go cold.
            val handle = cache.get(gavA).run
            ZIO.sleep(400.millis).run
            val warmAfterIdle = cache.warmCount.run
            // Touch the entry: should re-promote without invoking the
            // downloader again.
            val foo = handle.readEntryString("pkg/Foo.html").run
            val warmAfterAccess = cache.warmCount.run
            val n = callCount.get.map(_.getOrElse(gavA, 0)).run
            assertTrue(
              warmAfterIdle == 0,
              warmAfterAccess == 1,
              n == 1, // no re-download
              foo == "<html>Foo</html>",
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("idle TTL resets on access") {
      // Access the entry inside the idle window; it must stay warm.
      val body = ZIO.scoped:
        withTunedCache(warmIdleTtl = 300.millis, sweepInterval = 50.millis): (cache, _) =>
          defer:
            val handle = cache.get(gavA).run
            // Touch the handle every 100ms for ~500ms (well past one TTL
            // worth of idle time, but never actually idle for 300ms).
            ZIO.foreachDiscard(1 to 5): _ =>
              handle.hasEntry("index.html") *> ZIO.sleep(100.millis)
            .run
            val warmDuringTouch = cache.warmCount.run
            // Now stop touching and let it go cold.
            ZIO.sleep(500.millis).run
            val warmAfterIdle = cache.warmCount.run
            assertTrue(warmDuringTouch == 1, warmAfterIdle == 0)
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("an in-flight stream blocks demotion until it completes") {
      // The sweeper must not close a ZipFile that an in-flight reader
      // still holds. We start a stream, let the sweeper run several
      // times while the stream is active, then let the stream complete
      // and check that demotion happens after.
      val body = ZIO.scoped:
        withTunedCache(warmIdleTtl = 50.millis, sweepInterval = 25.millis): (cache, _) =>
          defer:
            val handle = cache.get(gavA).run
            // `streamEntry`'s scope keeps the entry's refCount at 1
            // for the lifetime of the consumer. Run the consumer
            // slowly (with a delay between elements) so the sweeper has
            // multiple chances to attempt demotion while we're holding.
            val streamFiber =
              handle
                .streamEntry("pkg/Foo.html", chunkSize = 1)
                .schedule(zio.Schedule.fixed(20.millis))
                .runDrain
                .fork
                .run
            // Let the sweeper run a few times during the stream.
            ZIO.sleep(200.millis).run
            val warmDuringStream = cache.warmCount.run
            // Wait for the stream to finish, then for one more sweep.
            streamFiber.join.run
            ZIO.sleep(150.millis).run
            val warmAfterStream = cache.warmCount.run
            assertTrue(
              warmDuringStream == 1, // refCount > 0 blocked demotion
              warmAfterStream == 0,  // refCount == 0; demoted
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("coldIdleTtl=None never evicts cold entries (default behavior)") {
      // With coldIdleTtl unset, an entry that has gone cold and stayed
      // idle indefinitely keeps its on-disk file and remains in the
      // cache map. Re-promotion still works without re-download.
      val body = ZIO.scoped:
        withTunedCache(warmIdleTtl = 50.millis, sweepInterval = 25.millis): (cache, callCount) =>
          defer:
            val handle = cache.get(gavA).run
            // Wait long enough for many sweep passes after the warm TTL.
            ZIO.sleep(500.millis).run
            val sized       = cache.size.run
            val warm        = cache.warmCount.run
            val downloads1  = callCount.get.map(_.getOrElse(gavA, 0)).run
            // Touch the entry; it must still be the same cached entry
            // (no re-download).
            val foo         = handle.readEntryString("pkg/Foo.html").run
            val downloads2  = callCount.get.map(_.getOrElse(gavA, 0)).run
            assertTrue(
              sized == 1,         // entry still in the cache
              warm == 0,          // demoted to cold
              downloads1 == 1,    // single original download
              downloads2 == 1,    // no re-download triggered by the touch
              foo == "<html>Foo</html>",
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("coldIdleTtl evicts cold entries; re-get re-downloads") {
      // With coldIdleTtl set short, an idle cold entry is fully evicted:
      // removed from the cache map, on-disk file deleted. A subsequent
      // get for the same GAV re-runs the downloader.
      val body = ZIO.scoped:
        withTunedCache(
          warmIdleTtl   = 50.millis,
          sweepInterval = 25.millis,
          coldIdleTtl   = Some(150.millis),
        ): (cache, callCount) =>
          defer:
            cache.get(gavA).run
            // After warmIdleTtl + coldIdleTtl + a sweep tick, the entry
            // should be evicted and removed from the map.
            ZIO.sleep(500.millis).run
            val sizedAfter      = cache.size.run
            val warmAfter       = cache.warmCount.run
            val downloadsBefore = callCount.get.map(_.getOrElse(gavA, 0)).run
            // Re-`get` triggers a full cache miss, which re-downloads
            // the jar (callCount increments).
            cache.get(gavA).run
            val downloadsAfter  = callCount.get.map(_.getOrElse(gavA, 0)).run
            assertTrue(
              sizedAfter == 0,         // entry was removed
              warmAfter == 0,
              downloadsBefore == 1,    // first miss
              downloadsAfter == 2,     // second miss: re-download
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("access during the cold window resets the idle timer and prevents eviction") {
      val body = ZIO.scoped:
        withTunedCache(
          warmIdleTtl   = 50.millis,
          sweepInterval = 25.millis,
          coldIdleTtl   = Some(150.millis),
        ): (cache, callCount) =>
          defer:
            val handle = cache.get(gavA).run
            // Touch the handle every 50ms for ~400ms — never idle for the
            // full coldIdleTtl. Each touch forces a brief warm period
            // (refCount + lastAccess update); the sweeper may demote
            // between touches but must NOT evict.
            ZIO.foreachDiscard(1 to 8): _ =>
              handle.hasEntry("index.html") *> ZIO.sleep(50.millis)
            .run
            val sized      = cache.size.run
            val downloads  = callCount.get.map(_.getOrElse(gavA, 0)).run
            assertTrue(
              sized == 1,        // entry still cached
              downloads == 1,    // never re-downloaded
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

    test("a JarHandle held across an eviction fails loudly on next operation") {
      // Callers are expected to re-`get` a fresh handle after eviction.
      // Operations on the stale handle die with a defect mentioning
      // both shutdown and cold-eviction.
      val body = ZIO.scoped:
        withTunedCache(
          warmIdleTtl   = 50.millis,
          sweepInterval = 25.millis,
          coldIdleTtl   = Some(150.millis),
        ): (cache, _) =>
          defer:
            val handle = cache.get(gavA).run
            ZIO.sleep(500.millis).run // long enough for warm-then-cold-then-evict
            val readResult = handle.readEntry("pkg/Foo.html").exit.run
            assertTrue(
              readResult.isFailure, // defect from `acquire`
            )
      body.provide(Client.default, MavenCentral.MavenCentralRepo.live)
    },

  ) @@ TestAspect.withLiveClock
