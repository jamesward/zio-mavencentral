package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.{GroupArtifactVersion, JarMeta, NotFoundError, retryOnServerError}
import zio.*
import zio.concurrent.ConcurrentMap
import zio.direct.*
import zio.http.{Client, URL}

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.zip.ZipFile

/**
 * On-disk jar cache with random-access reads.
 *
 * Each cached GAV owns one immutable `.jar` file and one open `ZipFile`
 * handle. Reads use `ZipFile`'s random-access path: a `getEntry` lookup
 * against the in-memory central directory plus a `RandomAccessFile.seek`
 * to the local file header. Only the requested entry is decompressed;
 * the rest of the jar is never touched on a hit.
 *
 * Lifetime model
 * --------------
 * Entries are append-only for the life of the cache. There is no
 * capacity-driven eviction and no TTL. The cache is intended to be sized
 * to fit the consumer's working set on whatever ephemeral storage the
 * deployment provides; a process restart is the natural reset.
 *
 * Open `ZipFile` handles are released only when the cache itself is
 * finalized. A request reading from a handle never races against
 * deletion, which closes a class of races present in cache designs that
 * delete extracted directories on eviction.
 *
 * Concurrency
 * -----------
 * Concurrent `get` calls for the same GAV are deduplicated via a
 * `Promise`-keyed map (single-flight). The owning fiber downloads and
 * opens the jar; waiters receive the same `JarHandle`. The owner
 * removes the pending entry and completes the promise on exit, even
 * on failure or interrupt.
 */
final class JarCache private (
  cacheDir: File,
  jars: ConcurrentMap[GroupArtifactVersion, JarCache.JarHandle],
  pendingFetches: ConcurrentMap[GroupArtifactVersion, Promise[NotFoundError, JarCache.JarHandle]],
  download: (GroupArtifactVersion, File) => ZIO[Client, NotFoundError, JarMeta],
  label: String,
):
  /**
   * Get (or fetch-and-open) the cached jar for `gav`.
   *
   * Cache hit  → returns the existing `JarHandle` immediately.
   * Cache miss → at most one fiber per GAV downloads + opens; concurrent
   *              callers for the same GAV await the same `Promise`.
   */
  def get(gav: GroupArtifactVersion): ZIO[Client, NotFoundError, JarCache.JarHandle] =
    defer:
      jars.get(gav).run match
        case Some(handle) =>
          handle
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

  private def fetchAndOpen(gav: GroupArtifactVersion): ZIO[Client, NotFoundError, JarCache.JarHandle] =
    defer:
      val jarFile = File(cacheDir, JarCache.jarFileName(gav))
      ZIO.logInfo(s"Downloading $label jar: $gav").run
      val (duration, meta) = download(gav, jarFile).timed.run
      val sizeBytes        = ZIO.attempt(jarFile.length()).orDie.run
      ZIO.logInfo(s"Downloaded $label jar: $gav size=${sizeBytes / 1024}KB duration=${duration.toMillis}ms").run

      val zipFile = ZIO.attemptBlockingIO(ZipFile(jarFile)).orDie.run
      val handle  = JarCache.JarHandle(gav, zipFile, sizeBytes, meta)
      jars.put(gav, handle).run
      handle

  /** Number of cached jars. */
  def size: UIO[Int] =
    jars.toChunk.map(_.size)

  /** Total bytes across all cached `.jar` files on disk. */
  def totalBytes: UIO[Long] =
    jars.toChunk.map(_.foldLeft(0L)((acc, kv) => acc + kv._2.sizeBytes))

  /** True if the GAV is currently cached. */
  def contains(gav: GroupArtifactVersion): UIO[Boolean] =
    jars.get(gav).map(_.isDefined)

  private[zio_mavencentral] def closeAll: UIO[Unit] =
    jars.toChunk.flatMap: kvs =>
      ZIO.foreachDiscard(kvs)((_, h) => h.close)

object JarCache:

  /** Failure type for entry lookups. Distinct from `NotFoundError`,
   *  which signals the *jar* was not found upstream. */
  case class JarEntryNotFound(gav: GroupArtifactVersion, path: String)

  /**
   * Read-only handle to one cached jar.
   *
   * `ZipFile` is documented thread-safe for concurrent reads against a
   * single instance, so no synchronization is needed at this layer.
   * `readEntry` does at most one decompression pass per call.
   */
  final class JarHandle private[zio_mavencentral] (
    val gav: GroupArtifactVersion,
    private val zipFile: ZipFile,
    val sizeBytes: Long,
    val meta: JarMeta,
  ):
    /** True if the jar contains an entry at `path`. O(1) lookup. */
    def hasEntry(path: String): UIO[Boolean] =
      ZIO.succeed(zipFile.getEntry(path) != null)

    /** Read one entry's bytes. Random-access into the jar; no full scan. */
    def readEntry(path: String): IO[JarEntryNotFound, Array[Byte]] =
      ZIO.attemptBlockingIO:
        val entry = zipFile.getEntry(path)
        if entry == null then null
        else
          val in = zipFile.getInputStream(entry).nn
          try in.readAllBytes() finally in.close()
      .orDie
      .flatMap:
        case null  => ZIO.fail(JarEntryNotFound(gav, path))
        case bytes => ZIO.succeed(bytes)

    /** Read one entry as a UTF-8 string. */
    def readEntryString(path: String): IO[JarEntryNotFound, String] =
      readEntry(path).map(new String(_, StandardCharsets.UTF_8))

    /** All entry names (files + directories). Does not decompress any data. */
    def entryNames: UIO[Set[String]] =
      ZIO.succeed:
        val it = zipFile.entries.nn
        val b  = Set.newBuilder[String]
        while it.hasMoreElements do
          b += it.nextElement().nn.getName.nn
        b.result()

    /** All entry names matching `predicate`. Single pass. */
    def filterEntryNames(predicate: String => Boolean): UIO[Set[String]] =
      ZIO.succeed:
        val it = zipFile.entries.nn
        val b  = Set.newBuilder[String]
        while it.hasMoreElements do
          val name = it.nextElement().nn.getName.nn
          if predicate(name) then b += name
        b.result()

    private[zio_mavencentral] def close: UIO[Unit] =
      ZIO.attemptBlockingIO(zipFile.close()).ignoreLogged

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
   * Production path; tests pass a different downloader that copies a
   * fixture jar into place without doing any network work.
   */
  def httpDownloader(
    uri: GroupArtifactVersion => ZIO[Client, NotFoundError | MavenCentral.TemporaryServerError | Throwable, URL],
  ): (GroupArtifactVersion, File) => ZIO[Client, NotFoundError, JarMeta] =
    (gav, target) =>
      val resolveUrl: ZIO[Client, NotFoundError, URL] =
        uri(gav)
          .retryOnServerError
          .catchAll:
            case t: Throwable      => ZIO.die(t)
            case nf: NotFoundError => ZIO.fail(nf)

      defer:
        val url = resolveUrl.run
        MavenCentral.downloadJar(url, target).orDie.run

  /**
   * Construct a `JarCache`. The returned ZIO requires a `Scope`; when
   * the scope closes (typically at app shutdown) all open `ZipFile`
   * handles are released.
   */
  def make(
    cacheDir: File,
    download: (GroupArtifactVersion, File) => ZIO[Client, NotFoundError, JarMeta],
    label: String = "javadoc",
  ): ZIO[Scope, Nothing, JarCache] =
    defer:
      ZIO.attemptBlockingIO:
        if !cacheDir.exists() then cacheDir.mkdirs()
        ()
      .orDie.run

      val jars    = ConcurrentMap.empty[GroupArtifactVersion, JarHandle].run
      val pending = ConcurrentMap.empty[GroupArtifactVersion, Promise[NotFoundError, JarHandle]].run
      val cache   = new JarCache(cacheDir, jars, pending, download, label)
      ZIO.addFinalizer(cache.closeAll).run
      cache
