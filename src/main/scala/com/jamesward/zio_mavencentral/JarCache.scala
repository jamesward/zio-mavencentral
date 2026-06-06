package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.{GroupArtifactVersion, JarMeta, MavenCentralRepo, NotFoundError}
import zio.*
import zio.concurrent.ConcurrentMap
import zio.direct.*
import zio.http.{Client, URL}
import zio.stream.ZStream

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.zip.ZipFile

/**
 * On-disk jar cache with random-access reads and idle-driven heap eviction.
 *
 * Each cached GAV owns one immutable `.jar` file on disk and — when warm —
 * one open `java.util.zip.ZipFile` handle. Reads use `ZipFile`'s
 * random-access path: a `getEntry` lookup against the in-memory central
 * directory plus a `RandomAccessFile.seek` to the local file header.
 *
 * Two-tier lifetime model
 * -----------------------
 * Each cache entry is in one of two states:
 *
 *   - '''Warm''': an open `ZipFile` is held in heap (central-directory
 *     bytes, entry-offset hash table, inflater state, file handle). All
 *     reads against the entry are served directly from this handle.
 *   - '''Cold''': only the on-disk `.jar` file and the entry's metadata
 *     are retained. The heap cost of the warm `ZipFile` has been
 *     reclaimed.
 *
 * Entries enter Warm on the first access (cache miss → download → open)
 * and on any subsequent access while Cold. A periodic sweeper demotes
 * Warm entries to Cold once they have not been accessed for
 * [[warmIdleTtl]]. The on-disk file persists as long as the cache itself
 * does — there is no disk-side eviction; process restart is the natural
 * reset.
 *
 * This is the model that fits caches dominated by long-tail diversity
 * with a stable hot set: the popular working set stays warm with no
 * per-request open cost, and rarely-touched entries stop pinning heap
 * after `warmIdleTtl` without losing the on-disk download.
 *
 * Concurrency
 * -----------
 * Cache misses are deduplicated per GAV via `Promise`-keyed single-flight
 * (the existing model). Each warm entry exposes its `ZipFile` through
 * an internal scoped acquire that atomically bumps a per-entry refcount
 * and returns the live `ZipFile`; the scope close decrements.
 *
 * The sweeper attempts to demote in a single atomic `Ref.modify` that
 * swaps `(refCount == 0 && idle, Some(zip))` to `(_, None)`. Either:
 *
 *   - The sweeper's modify wins: `state.zip` becomes `None`. Any reader
 *     entering after this point sees Cold, takes the promotion mutex,
 *     opens a fresh `ZipFile`, and proceeds. The taken old `ZipFile`
 *     is closed by the sweeper.
 *   - A reader's modify wins first: `refCount > 0`; the sweeper observes
 *     this and does nothing this round.
 *
 * There is no window in which the sweeper closes a `ZipFile` while an
 * in-flight read holds it.
 */
final class JarCache private (
  cacheDir: File,
  jars: ConcurrentMap[GroupArtifactVersion, JarCache.CacheEntry],
  pendingFetches: ConcurrentMap[GroupArtifactVersion, Promise[NotFoundError, JarCache.JarHandle]],
  download: (GroupArtifactVersion, File) => ZIO[Client & MavenCentralRepo, NotFoundError, JarMeta],
  label: String,
  warmIdleTtl: Duration,
):
  /**
   * Get (or fetch-and-open) the cached jar for `gav`.
   *
   *   - Cache hit (Warm or Cold): returns the existing [[JarCache.JarHandle]]
   *     immediately. If the entry is Cold, the next operation on the
   *     handle will transparently re-promote it to Warm.
   *   - Cache miss: at most one fiber per GAV downloads + opens; concurrent
   *     callers for the same GAV await the same `Promise`.
   */
  def get(gav: GroupArtifactVersion): ZIO[Client & MavenCentralRepo, NotFoundError, JarCache.JarHandle] =
    defer:
      jars.get(gav).run match
        case Some(entry) =>
          JarCache.JarHandle(entry)
        case None =>
          val myPromise    = Promise.make[NotFoundError, JarCache.JarHandle].run
          val maybeWaiting = pendingFetches.putIfAbsent(gav, myPromise).run
          maybeWaiting match
            case Some(inFlight) =>
              ZIO.logInfo(s"Awaiting in-flight $label jar fetch: $gav").run
              inFlight.await.run
            case None =>
              fetchAndOpen(gav)
                .onExit: exit =>
                  pendingFetches.remove(gav) *> myPromise.done(exit)
                .run

  /**
   * Cache miss path: download the jar, open a `ZipFile` for it, and
   * register a new [[JarCache.CacheEntry]] in `jars`.
   *
   * The fresh entry starts Warm with `refCount = 0`: the next caller
   * (typically the same fiber, immediately) will bump `refCount` via
   * `acquire` for its own operation. If no such caller arrives before
   * `warmIdleTtl` elapses, the sweeper will demote it.
   */
  private def fetchAndOpen(gav: GroupArtifactVersion): ZIO[Client & MavenCentralRepo, NotFoundError, JarCache.JarHandle] =
    defer:
      val jarFile = File(cacheDir, JarCache.jarFileName(gav))
      ZIO.logInfo(s"Downloading $label jar: $gav").run
      val (duration, meta) = download(gav, jarFile).timed.run
      val sizeBytes        = ZIO.attempt(jarFile.length()).orDie.run
      ZIO.logInfo(s"Downloaded $label jar: $gav size=${sizeBytes / 1024}KB duration=${duration.toMillis}ms").run

      val zipFile        = ZIO.attemptBlockingIO(ZipFile(jarFile)).orDie.run
      val state          = Ref.make(JarCache.EntryState(refCount = 0, zip = Some(zipFile))).run
      val promotionMutex = Semaphore.make(1).run
      val lastAccess     = Ref.make(java.lang.System.nanoTime()).run
      val entry          = JarCache.CacheEntry(gav, jarFile, sizeBytes, meta, state, promotionMutex, lastAccess)
      jars.put(gav, entry).run
      JarCache.JarHandle(entry)

  /** Number of cached jars (warm + cold). */
  def size: UIO[Int] =
    jars.toChunk.map(_.size)

  /** Total bytes across all cached `.jar` files on disk. */
  def totalBytes: UIO[Long] =
    jars.toChunk.map(_.foldLeft(0L)((acc, kv) => acc + kv._2.sizeBytes))

  /**
   * Number of currently warm entries (those holding an open `ZipFile`).
   * Counterpart to [[size]] (which counts warm + cold). The difference
   * is the count of entries whose heap cost has been reclaimed by the
   * sweeper but whose on-disk `.jar` is still cached.
   */
  def warmCount: UIO[Int] =
    jars.toChunk.flatMap: kvs =>
      ZIO.foreach(kvs)((_, e) => e.isWarm)
        .map(_.count(identity))

  /** True if the GAV is currently cached (in either state). */
  def contains(gav: GroupArtifactVersion): UIO[Boolean] =
    jars.get(gav).map(_.isDefined)

  /**
   * Sweeper body: scans every entry once and demotes any that have been
   * idle for at least `warmIdleTtl`. Returns the number of entries
   * demoted in this pass — useful for logging and tests.
   */
  private[zio_mavencentral] def sweep: UIO[Int] =
    val ttlNanos = warmIdleTtl.toNanos
    jars.toChunk.flatMap: kvs =>
      val now = java.lang.System.nanoTime()
      ZIO.foreach(kvs)((_, e) => e.tryDemote(now, ttlNanos, label))
        .map(_.count(identity))

  /**
   * Cache-shutdown finalizer. Closes every entry's `ZipFile` (if warm)
   * and marks the entry permanently closed so that any further attempt
   * to `acquire` it dies with a defect rather than re-opening a fresh
   * `ZipFile` against the on-disk file.
   *
   * On-disk files are intentionally not deleted: callers may want to
   * inspect them post-shutdown (and Heroku-style ephemeral filesystems
   * reclaim them with the dyno).
   */
  private[zio_mavencentral] def closeAll: UIO[Unit] =
    jars.toChunk.flatMap: kvs =>
      ZIO.foreachDiscard(kvs)((_, e) => e.shutdown)

object JarCache:

  /** Failure type for entry lookups. Distinct from `NotFoundError`,
   *  which signals the *jar* was not found upstream. */
  case class JarEntryNotFound(gav: GroupArtifactVersion, path: String)

  /**
   * Internal hot-path state for a [[CacheEntry]]. Held in a single `Ref`
   * so that `state.modify` can atomically combine the refcount bump with
   * the warm/cold check, removing the close-mid-read race.
   *
   * Invariants:
   *   - `refCount >= 0`
   *   - `refCount > 0`  ⇒  `zip.isDefined` (a reader can't be holding a
   *     non-existent `ZipFile`)
   *   - The sweeper will only demote when `refCount == 0`.
   */
  private[zio_mavencentral] case class EntryState(refCount: Int, zip: Option[ZipFile])

  /**
   * One slot in the cache. Holds the on-disk file location + metadata
   * permanently, and the open `ZipFile` only while warm.
   *
   * Most users interact with this through [[JarHandle]]; the entry is
   * exposed package-private so tests (and the cache itself) can drive
   * its state machine directly.
   */
  private[zio_mavencentral] final class CacheEntry(
    val gav: GroupArtifactVersion,
    val file: File,
    val sizeBytes: Long,
    val meta: JarMeta,
    state: Ref[EntryState],
    promotionMutex: Semaphore,
    lastAccess: Ref[Long],
  ):
    /**
     * Volatile flag: set to true by [[shutdown]] to prevent any further
     * promotion. A `Ref[Boolean]` would also work but a plain
     * `AtomicBoolean` keeps the read off the ZIO runtime on the hot
     * acquire path.
     */
    private val closedFlag: java.util.concurrent.atomic.AtomicBoolean =
      java.util.concurrent.atomic.AtomicBoolean(false)

    /**
     * Acquire the entry's `ZipFile` under a [[Scope]]. Bumps `refCount`
     * on entry, decrements on scope close. The `ZipFile` is guaranteed
     * not to be closed by the sweeper for the lifetime of the scope.
     *
     * Promotes Cold→Warm if necessary. Promotion is single-flight via
     * `promotionMutex`: if multiple fibers see Cold concurrently, only
     * one opens a new `ZipFile`; the rest observe the resulting Warm
     * state and proceed.
     *
     * Failure modes are all defects (`die`):
     *   - Cache shut down: the operator misused a closed cache.
     *   - `ZipFile` constructor I/O failure: the on-disk file is corrupt
     *     or the FS is broken; not something a caller can recover from.
     */
    def acquire: ZIO[Scope, Nothing, ZipFile] =
      ZIO.acquireRelease(
        // Acquire: timestamp + atomically read-or-bump under state.modify.
        defer:
          // Refuse to acquire on a shut-down cache. This catches the rare
          // case where someone holds a `JarHandle` past the cache's scope
          // closure; without it we'd happily re-open a fresh `ZipFile`
          // against a file the caller is no longer entitled to use.
          if closedFlag.get() then
            ZIO.dieMessage(s"JarCache entry $gav used after cache shutdown").run

          lastAccess.set(java.lang.System.nanoTime()).run
          val maybeWarmZip =
            state.modify: s =>
              s.zip match
                case Some(zip) => (Some(zip), s.copy(refCount = s.refCount + 1))
                case None      => (None, s)
            .run
          maybeWarmZip match
            case Some(zip) => zip
            case None      => promote.run
      )(_ => state.update(s => s.copy(refCount = s.refCount - 1)))

    /**
     * Open a fresh `ZipFile` for this entry, transitioning Cold→Warm.
     * Single-flight via `promotionMutex` to dedupe concurrent
     * cold-path callers; under the mutex we re-check `state` because
     * another fiber may have promoted while we were queued.
     *
     * On entry: caller has *not* bumped `refCount` (the cold path
     * couldn't, since `state.modify` saw `None`). On exit: `refCount`
     * has been bumped exactly once (by us), and `state.zip` is `Some`.
     */
    private def promote: ZIO[Any, Nothing, ZipFile] =
      promotionMutex.withPermit:
        defer:
          val current = state.get.run
          current.zip match
            case Some(zip) =>
              // Another fiber promoted while we were waiting on the mutex.
              // Just bump refCount against their ZipFile.
              state.update(s => s.copy(refCount = s.refCount + 1)).run
              zip
            case None =>
              val newZip = ZIO.attemptBlockingIO(ZipFile(file)).orDie.run
              state.update(s => s.copy(refCount = s.refCount + 1, zip = Some(newZip))).run
              newZip

    /** True if currently warm (open `ZipFile` resident). For diagnostics. */
    def isWarm: UIO[Boolean] =
      state.get.map(_.zip.isDefined)

    /**
     * Sweeper hook: try to demote this entry from Warm to Cold if it has
     * been idle for at least `idleTtlNanos` *and* no readers currently
     * hold its `ZipFile`. Returns `true` iff a `ZipFile` was actually
     * closed by this call.
     *
     * Atomicity is provided by `state.modify`: any reader's `acquire`
     * runs through the same `Ref`, so either the sweeper observes
     * `refCount == 0` and takes the `ZipFile` away in the same atomic
     * step, or a reader's bump wins first and the sweeper sees
     * `refCount > 0` and skips. Closing the taken `ZipFile` outside the
     * `Ref.modify` is safe because no other reference to it exists once
     * we've swapped `state.zip` to `None`.
     */
    private[zio_mavencentral] def tryDemote(now: Long, idleTtlNanos: Long, label: String): UIO[Boolean] =
      defer:
        val isIdle = (now - lastAccess.get.run) > idleTtlNanos
        if !isIdle then false
        else
          val taken =
            state.modify: s =>
              if s.refCount == 0 && s.zip.isDefined then (s.zip, s.copy(zip = None))
              else (None, s)
            .run
          taken match
            case Some(zip) =>
              ZIO.logInfo(s"Demoting $label jar to cold: $gav").run
              ZIO.attemptBlockingIO(zip.close()).ignoreLogged.run
              true
            case None =>
              false

    /**
     * Cache-shutdown hook: close any resident `ZipFile` and mark this
     * entry permanently closed. No new `acquire` will succeed after this
     * runs.
     *
     * In-flight readers that already obtained a `ZipFile` reference
     * before `shutdown` will be reading from a closed `ZipFile` (or
     * having their reads fail with `IllegalStateException`); that's the
     * same observable behavior as the previous append-only cache.
     */
    private[zio_mavencentral] def shutdown: UIO[Unit] =
      defer:
        closedFlag.set(true)
        val taken =
          state.modify: s =>
            (s.zip, s.copy(zip = None))
          .run
        ZIO.foreachDiscard(taken)(zip => ZIO.attemptBlockingIO(zip.close()).ignoreLogged).run

  /**
   * Read-only handle to one cached jar. Logical pointer to a
   * [[CacheEntry]]; obtaining a handle does not pin the entry's
   * `ZipFile` open — each operation on the handle does that for the
   * duration of the operation (or, for [[streamEntry]], the duration
   * of the resulting stream).
   *
   * `ZipFile` is documented thread-safe for concurrent reads against a
   * single instance, so multiple concurrent operations through a single
   * handle are safe.
   */
  final class JarHandle private[zio_mavencentral] (private val entry: CacheEntry):
    def gav: GroupArtifactVersion = entry.gav
    def sizeBytes: Long           = entry.sizeBytes
    def meta: JarMeta             = entry.meta

    /** True if the jar contains an entry at `path`. O(1) lookup. */
    def hasEntry(path: String): UIO[Boolean] =
      ZIO.scoped:
        entry.acquire.flatMap: zip =>
          ZIO.attemptBlockingIO(zip.getEntry(path) != null).orDie

    /** Read one entry's bytes. Random-access into the jar; no full scan. */
    def readEntry(path: String): IO[JarEntryNotFound, Array[Byte]] =
      ZIO.scoped:
        entry.acquire.flatMap: zip =>
          ZIO.attemptBlockingIO:
            val e = zip.getEntry(path)
            if e == null then null
            else
              val in = zip.getInputStream(e).nn
              try in.readAllBytes() finally in.close()
          .orDie
          .flatMap:
            case null  => ZIO.fail(JarEntryNotFound(entry.gav, path))
            case bytes => ZIO.succeed(bytes)

    /** Read one entry as a UTF-8 string. */
    def readEntryString(path: String): IO[JarEntryNotFound, String] =
      readEntry(path).map(new String(_, StandardCharsets.UTF_8))

    /**
     * Stream one entry's bytes without buffering the whole entry in heap.
     *
     * Backed by the cached `ZipFile`'s `InputStream` for the requested
     * entry, read in `chunkSize` byte chunks. The entry's refcount and
     * the `InputStream` are owned by the resulting stream's scope; both
     * are released when the stream terminates (success, failure, or
     * interrupt). Use this for serving large jar entries over HTTP —
     * [[readEntry]] allocates a single `byte[]` of the entry's full
     * uncompressed size, which is fine for small files but scales poorly
     * under concurrency on multi-MB entries.
     *
     * The entry refcount held for the lifetime of this stream prevents
     * the cache sweeper from closing the underlying `ZipFile` mid-read.
     */
    def streamEntry(path: String, chunkSize: Int = 64 * 1024): ZStream[Any, JarEntryNotFound, Byte] =
      ZStream.unwrapScoped:
        entry.acquire.map: zip =>
          ZStream.unwrap:
            ZIO.attemptBlockingIO(zip.getEntry(path)).orDie.map: e =>
              if e == null then ZStream.fail(JarEntryNotFound(entry.gav, path))
              else
                ZStream
                  .fromInputStreamScoped(
                    ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(zip.getInputStream(e).nn)),
                    chunkSize,
                  )
                  .orDie

    /** All entry names (files + directories). Does not decompress any data. */
    def entryNames: UIO[Set[String]] =
      ZIO.scoped:
        entry.acquire.flatMap: zip =>
          ZIO.attemptBlockingIO:
            val it = zip.entries.nn
            val b  = Set.newBuilder[String]
            while it.hasMoreElements do
              b += it.nextElement().nn.getName.nn
            b.result()
          .orDie

    /** All entry names matching `predicate`. Single pass. */
    def filterEntryNames(predicate: String => Boolean): UIO[Set[String]] =
      ZIO.scoped:
        entry.acquire.flatMap: zip =>
          ZIO.attemptBlockingIO:
            val it = zip.entries.nn
            val b  = Set.newBuilder[String]
            while it.hasMoreElements do
              val name = it.nextElement().nn.getName.nn
              if predicate(name) then b += name
            b.result()
          .orDie

  /**
   * Map a GAV to a flat filename. Slashes in `gav.toString` would collide
   * with directory paths; replace them so all jars live under one dir.
   */
  private[zio_mavencentral] def jarFileName(gav: GroupArtifactVersion): String =
    s"${gav.toString.replace('/', '_')}.jar"

  /**
   * Concrete downloader: resolves `gav` → URL via `MavenCentral.javadocUri`
   * (or `sourcesUri`), then streams the jar bytes to `target` via
   * [[MavenCentral.downloadJar]].
   *
   * URL resolution goes through [[MavenCentralRepo]], so transient
   * upstream failures (5xx / 429 / 403) trigger mirror fallback and
   * count toward each mirror's circuit breaker. There is no per-call
   * retry — failure of the resolve step is surfaced directly.
   *
   * The download step (`MavenCentral.downloadJar`) hits the resolved URL
   * directly via [[zio.http.Client]], so the returned function still
   * requires `Client` in its environment.
   *
   * Production path; tests pass a different downloader that copies a
   * fixture jar into place without doing any network work.
   */
  def httpDownloader(
    uri: GroupArtifactVersion => ZIO[MavenCentralRepo, NotFoundError | MavenCentral.TemporaryServerError | Throwable, URL],
  ): (GroupArtifactVersion, File) => ZIO[Client & MavenCentralRepo, NotFoundError, JarMeta] =
    (gav, target) =>
      val resolveUrl: ZIO[MavenCentralRepo, NotFoundError, URL] =
        uri(gav)
          .catchAll:
            case t: Throwable      => ZIO.die(t)
            case nf: NotFoundError => ZIO.fail(nf)

      defer:
        val url = resolveUrl.run
        MavenCentral.downloadJar(url, target).orDie.run

  /**
   * Construct a `JarCache`. The returned ZIO requires a [[Scope]]; when
   * the scope closes (typically at app shutdown):
   *
   *   1. The background sweeper fiber is interrupted.
   *   2. All resident `ZipFile` handles are released and entries are
   *      marked closed (so any caller still holding a [[JarHandle]]
   *      will get a defect on the next operation rather than re-opening
   *      against the on-disk file).
   *
   * @param warmIdleTtl     How long an entry may remain warm without
   *                        being accessed before the sweeper demotes it
   *                        to cold and closes its `ZipFile`. The
   *                        sliding window resets on every operation.
   * @param sweepInterval   How often the demotion sweeper runs. Should
   *                        be at most `warmIdleTtl / 2` for the actual
   *                        idle time before demotion to be close to
   *                        `warmIdleTtl`.
   */
  def make(
    cacheDir: File,
    download: (GroupArtifactVersion, File) => ZIO[Client & MavenCentralRepo, NotFoundError, JarMeta],
    label: String = "javadoc",
    warmIdleTtl: Duration = 30.minutes,
    sweepInterval: Duration = 1.minute,
  ): ZIO[Scope, Nothing, JarCache] =
    defer:
      ZIO.attemptBlockingIO:
        if !cacheDir.exists() then cacheDir.mkdirs()
        ()
      .orDie.run

      val jars    = ConcurrentMap.empty[GroupArtifactVersion, CacheEntry].run
      val pending = ConcurrentMap.empty[GroupArtifactVersion, Promise[NotFoundError, JarHandle]].run
      val cache   = new JarCache(cacheDir, jars, pending, download, label, warmIdleTtl)

      // Sweeper runs for the life of the cache scope; the close-all
      // finalizer below also covers the case where the sweeper hasn't
      // had a chance to demote anything by shutdown time.
      cache.sweep
        .repeat(Schedule.fixed(sweepInterval))
        .forkScoped
        .run

      ZIO.addFinalizer(cache.closeAll).run
      cache
