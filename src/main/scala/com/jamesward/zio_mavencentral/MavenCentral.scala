package com.jamesward.zio_mavencentral

import nl.vroste.rezilience.*
import nl.vroste.rezilience.CircuitBreaker.{CircuitBreakerOpen, WrappedError}
import org.bouncycastle.bcpg.{ArmoredOutputStream, BCPGOutputStream, HashAlgorithmTags}
import org.bouncycastle.openpgp.operator.jcajce.{JcaKeyFingerprintCalculator, JcaPGPContentSignerBuilder, JcePBESecretKeyDecryptorBuilder}
import org.bouncycastle.openpgp.{PGPPrivateKey, PGPPublicKey, PGPSecretKeyRing, PGPSignature, PGPSignatureGenerator}
import zio.direct.*
import zio.http.*
import zio.http.codec.PathCodec
import zio.schema.{Schema, derived}
import zio.schema.annotation.description
import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Chunk, ChunkBuilder, Duration, IO, LogAnnotation, Ref, Schedule, Scope, Trace, UIO, ZIO, ZLayer, durationInt, duration2DurationOps}

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.ZonedDateTime
import java.util.Base64
import scala.annotation.{targetName, unused}
import scala.util.matching.Regex
import scala.xml.Elem

object MavenCentral:

  // Maven Central's URLs. repo1 (behind Cloudflare) is the source of truth.
  // Google's GCS copy is the mirror Central lists in its own
  // `/.meta/repository-metadata.xml`. `repo.maven.apache.org` is the same
  // Cloudflare endpoint as repo1 (same IPs), so it's no fallback and isn't used
  // by default. See `defaultMirrorSpecs` for how they're used.
  val primaryUri: URL  = URL.decode("https://repo1.maven.org/maven2/").toOption.get
  val gcsMirror: URL   = URL.decode("https://maven-central.storage-download.googleapis.com/maven2/").toOption.get
  val apacheAlias: URL = URL.decode("https://repo.maven.apache.org/maven2/").toOption.get

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

    /**
     * Maven-compatible version ordering from coursier (`coursier.version.Version`,
     * the same order as Maven's resolver): numeric segments compare numerically
     * (`2.10 > 2.9`) and qualifiers rank `dev < alpha < beta < pre < milestone < rc
     * < snapshot < release < ga < final < sp` (`2.0.0-RC2 < 2.0.0`). Plain string
     * order is wrong for both.
     */
    given ordering: Ordering[Version] = Ordering.by[Version, coursier.version.Version](coursier.version.Version(_))

    // Pre-release words coursier treats as plain literals rather than qualifiers.
    private val extraPreReleaseTags = Set("preview", "ea", "eap", "nightly", "incubating")

    /**
     * True for dev / alpha / beta / milestone / release-candidate / snapshot builds
     * (coursier's pre-release qualifiers, including `a1`/`b1`/`m1` abbreviations),
     * plus preview / early-access / nightly / incubating builds. `33.7.2-jre`,
     * `4.2.18.Final`, `5.0.0.RELEASE` and `2.7.0-SP1` are releases.
     */
    def isPreRelease(v: Version): Boolean =
      coursier.version.Version(v).items.exists:
        case tag: coursier.version.Version.Tag => tag.isPreRelease || extraPreReleaseTags.contains(tag.value)
        case _                                 => false

  case class ArtifactAndVersion(artifactId: ArtifactId, maybeVersion: Option[Version] = None)
  @description("A Maven Central artifact coordinate: a groupId and artifactId, without a version.")
  case class GroupArtifact(
    @description("The Maven groupId, e.g. \"dev.zio\".")
    groupId: GroupId,
    @description("The Maven artifactId, e.g. \"zio_3\".")
    artifactId: ArtifactId,
  ):
    lazy val toPath: Path = groupId / artifactId

    @targetName("slash")
    @unused
    def /(version: Version): Path = toPath / version

  object GroupArtifact:
    // The ambient, canonical schema is the record form (derived in
    // `MavenCentralSchemas`, outside this object, so the opaque field types
    // stay opaque and their `@description`s are readable). `schema` is also
    // exposed as a plain `val` for consumers that shadow the given with their
    // own (e.g. a string codec).
    val schema: Schema[GroupArtifact] = MavenCentralSchemas.groupArtifact
    given Schema[GroupArtifact] = schema

  @description("A fully-qualified Maven Central artifact coordinate: groupId, artifactId, and version.")
  case class GroupArtifactVersion(
    @description("The Maven groupId, e.g. \"dev.zio\".")
    groupId: GroupId,
    @description("The Maven artifactId, e.g. \"zio_3\".")
    artifactId: ArtifactId,
    @description("The artifact version, e.g. \"2.1.9\".")
    version: Version,
  ):
    @unused
    lazy val noVersion: GroupArtifact = GroupArtifact(groupId, artifactId)
    @unused
    lazy val toPath: Path = groupId / artifactId / version
    override def toString: String = s"$groupId/$artifactId/$version"

  object GroupArtifactVersion:
    val schema: Schema[GroupArtifactVersion] = MavenCentralSchemas.groupArtifactVersion
    given Schema[GroupArtifactVersion] = schema

  /** A value with the validators of the response it came from, and the URL that
   *  served it (`servedBy` is on a fallback mirror when the authoritative one
   *  couldn't answer, which a cache may want to keep for less time). */
  case class WithCacheInfo[A](value: A, maybeLastModified: Option[ZonedDateTime], maybeEtag: Option[Header.ETag], servedBy: Option[URL] = None)

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
  /** Maven Central couldn't be reached for this `GroupArtifact`'s versions: a
   *  network failure, or a 5xx / 429 / 403 from every mirror. Temporary, so
   *  worth retrying. Surfaced by [[latestOrFail]]. */
  case class LatestUnavailable(groupArtifact: GroupArtifact, reason: String)
  /** A transient HTTP failure that's worth retrying — 5xx server errors,
   *  429 Too Many Requests, or 403 Forbidden. Maven Central throttles
   *  aggressive clients with both 429s and 403s; treating both as
   *  transient lets the mirror fallback (and the per-mirror circuit
   *  breaker in [[MavenCentralRepo]]) react. */
  case class TemporaryServerError(response: Response) extends Throwable(s"${response.status.code}: ${response.status.text}")
  private case class UnknownError(response: Response) extends Throwable(response.status.text)
  private case class ParseError(t: Throwable) extends Throwable(t)

  /** Status codes meaning Maven Central has nothing at the path: 404, plus the
   *  400 / 414 it answers for a path it can't parse (a NUL or invalid UTF-8 in
   *  a segment, a segment over ~1,100 characters). Such a path can't name a
   *  published artifact, so it's "not found", not an error. The mirrors
   *  disagree on which inputs get 400 vs 404 (repo1 answers 400 for a NUL, the
   *  GCS mirror 404; the reverse for a newline). */
  private[zio_mavencentral] def isAbsent(status: Status): Boolean =
    status == Status.NotFound || status == Status.BadRequest || status == Status.RequestUriTooLong

  /** Status codes that should cause a fallback to the next mirror and
   *  count toward tripping the per-mirror circuit breaker. Permanent 4xx
   *  (404, 401, 410, …) intentionally do **not** match — there is no point
   *  thrashing through every mirror looking for an artifact that doesn't
   *  exist. */
  private def isFallbackWorthy(status: Status): Boolean =
    status.isServerError || status.code == 429 || status.code == 403

  // Promote a non-success response to either a TemporaryServerError (retryable
  // / fallback-worthy) or an UnknownError (terminal). Centralizing the rule
  // means new fallback-worthy status codes only need to be added in one place.
  private[zio_mavencentral] def temporaryOrUnknown(response: Response): Throwable =
    if isFallbackWorthy(response.status) then TemporaryServerError(response)
    else UnknownError(response)

  /** Convenience constructor: `gav("org.foo", "bar", "1.2.3")`. */
  def gav(groupId: String, artifactId: String, version: String): GroupArtifactVersion =
    GroupArtifactVersion(GroupId(groupId), ArtifactId(artifactId), Version(version))

  // Schema givens live in `MavenCentralSchemas` (below, outside this object)
  // where the opaque types are opaque, so they are genuinely typed; the
  // GroupArtifact/GroupArtifactVersion record schemas are also surfaced as
  // ambient givens via their companions above.

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
                                kind: RequestKind = RequestKind.Fresh,
                              )(implicit trace: Trace): ZIO[MavenCentralRepo, Throwable, (Response, URL)] =
      ZIO.serviceWithZIO[MavenCentralRepo](_.request(path, method, headers, content, kind))

  /**
   * What a request reads, which decides the order the mirrors are tried in.
   * The authoritative mirror (repo1) is the only one whose "not found" is
   * believed: a fallback mirror (Google's GCS copy) lags new artifacts by up to
   * a day and serves no directory listings.
   */
  enum RequestKind:
    /** Changing metadata (`maven-metadata.xml`): the authoritative mirror first, for freshness. */
    case Fresh
    /** A published file that never changes (jar, POM): fallback mirrors first, to keep the
     *  bulk of the bytes off repo1; the authoritative mirror when they don't have it. */
    case Immutable
    /** A directory listing: only mirrors that serve HTML listings (see [[MavenCentralRepo.list]]). */
    case Listing

  /** How a mirror can list a directory's subdirectories. */
  enum Listings:
    /** An HTML index page at the directory's URL (repo1). */
    case Html
    /** A GCS bucket's JSON listing API (`prefix` + `delimiter=/`), at `api`. */
    case GcsBucket(api: URL, bucket: String, prefix: String)
    /** None. */
    case Unsupported

  /** One Maven Central mirror. Exactly one should be `authoritative`. */
  final case class Mirror(url: URL, authoritative: Boolean, listings: Listings)

  /** repo1: authoritative, fresh, serves listings, behind Cloudflare. */
  val primaryMirror: Mirror = Mirror(primaryUri, authoritative = true, Listings.Html)

  /** Google's GCS copy of Maven Central (the mirror Central itself lists in
   *  `/.meta/repository-metadata.xml`). Reliable, but lags new artifacts and has
   *  no HTML listings; its bucket API lists directories. */
  val gcsMirrorSpec: Mirror = Mirror(gcsMirror, authoritative = false,
    Listings.GcsBucket(URL.decode("https://storage.googleapis.com/storage/v1/b/").toOption.get, "maven-central", "maven2/"))

  val defaultMirrorSpecs: List[Mirror] = List(primaryMirror, gcsMirrorSpec)

  /** A fallback mirror doesn't have the path, and the authoritative mirror
   *  couldn't be asked (failing, or over its request budget). Not a "not found":
   *  the path may exist but be too new for the fallback. Temporary. */
  final case class NotFoundOnFallback(url: URL)
    extends IOException(s"${url.encode} isn't on the fallback mirror, and the authoritative mirror couldn't confirm it")

  /** Every mirror that could answer is failing or skipped. Temporary. */
  final case class AllMirrorsUnavailable(path: Path)
    extends IOException(s"no Maven Central mirror could answer for ${path.encode}")

  /** The result of listing a directory: its subdirectory names, or nothing there
   *  (only the authoritative mirror can say that). */
  enum Listed:
    case Names(names: Seq[String], maybeLastModified: Option[ZonedDateTime], url: URL)
    case Absent(url: URL)

  /** A mirror's breaker trips when at least `failureRate` of its calls fail within
   *  `window`, once it has had at least `minCalls` (rezilience evaluates a full window). */
  final case class BreakerSettings(failureRate: Double = 0.5, window: Duration = 30.seconds, minCalls: Int = 10)

  /** How many requests per second repo1 gets at most, and how long a request
   *  that needs its answer (an authoritative "not found") waits for budget. */
  final case class RateLimit(perSecond: Double = 20, burst: Int = 50, authoritativeWait: Duration = 2.seconds)

  /**
   * Maven Central requests with mirror fallback, per-mirror circuit breakers, and a
   * request budget for the authoritative mirror.
   *
   * Order (see [[RequestKind]]):
   *   - `Fresh`: repo1, then GCS. If repo1 is over budget or failing, GCS answers;
   *     a GCS "not found" then waits briefly for repo1's budget and asks it, and
   *     fails with [[NotFoundOnFallback]] (temporary) if it still can't.
   *   - `Immutable`: GCS, then repo1 (waiting briefly for budget) when GCS doesn't
   *     have the file or fails.
   *   - `Listing`: mirrors that serve HTML listings only.
   *
   * Only the authoritative mirror's 404 / 400 / 414 reaches the caller as a status;
   * a fallback's is never final. A response with a status matching
   * [[isFallbackWorthy]] (5xx / 429 / 403), an I/O error, or a timeout counts
   * against that mirror's breaker and moves on. A breaker trips when at least half
   * of its calls fail over 30 s (with at least 10 calls), probes again after 30 s,
   * and backs off to 10 minutes. Breaker state changes are logged at WARN. When no
   * mirror can answer, the call fails with [[AllMirrorsUnavailable]].
   */
  trait MavenCentralRepo:
    /** A request whose body is read into memory. */
    def request(
      path: Path,
      method: Method = Method.GET,
      headers: Headers = Headers.empty,
      content: Body = Body.empty,
      kind: RequestKind = RequestKind.Fresh,
    ): ZIO[Any, Throwable, (Response, URL)]

    /** A request whose body streams; the response lives until the `Scope` closes. */
    def streaming(
      path: Path,
      method: Method = Method.GET,
      headers: Headers = Headers.empty,
      kind: RequestKind = RequestKind.Immutable,
    ): ZIO[Scope, Throwable, (Response, URL)]

    /** A directory's subdirectory names: the HTML listing of a mirror that has
     *  one, else a GCS bucket listing (within `timeout`). */
    def list(path: Path, timeout: Duration = 10.seconds): ZIO[Any, Throwable, Listed]

    /** Requests answered by a fallback because the authoritative mirror was over
     *  its budget, since the layer started. */
    def rateLimited: UIO[Long]

  object MavenCentralRepo:

    // Required for matching `case CircuitBreakerOpen` under -language:strictEquality.
    private given CanEqual[CircuitBreaker.CircuitBreakerCallError[?], CircuitBreaker.CircuitBreakerCallError[?]] = CanEqual.derived
    private given CanEqual[RequestKind, RequestKind] = CanEqual.derived
    private given CanEqual[Listings, Listings] = CanEqual.derived

    /** Errors that count toward tripping the per-mirror circuit breaker. */
    private val trippingError: PartialFunction[Throwable, Boolean] =
      case _: TemporaryServerError => true
      case _: IOException          => true

    private final case class Live(mirror: Mirror, breaker: CircuitBreaker[Throwable])

    /** Default layer: repo1 (authoritative) and Google's GCS mirror, a 5 s
     *  per-attempt timeout, and a 20 requests/s (burst 50) budget for repo1. */
    val live: ZLayer[Client, Nothing, MavenCentralRepo] =
      custom()

    /**
     * Build a layer with custom mirrors and tuning.
     *
     * @param mirrors        Mirrors; exactly one should be authoritative.
     * @param requestTimeout Per-attempt timeout to get a response (a whole body
     *                       for [[MavenCentralRepo.request]], the headers for
     *                       [[MavenCentralRepo.streaming]]). A timeout counts
     *                       against the mirror's breaker and moves on.
     * @param rateLimit      The authoritative mirror's request budget.
     * @param resetMin       Open-state duration before a breaker probes again.
     * @param resetMax       Cap on the open-state duration as it backs off.
     * @param breaker        When a mirror's breaker trips.
     */
    def custom(
      mirrors: List[Mirror]    = defaultMirrorSpecs,
      requestTimeout: Duration = 5.seconds,
      rateLimit: RateLimit     = RateLimit(),
      resetMin: Duration       = 30.seconds,
      resetMax: Duration       = 10.minutes,
      breaker: BreakerSettings = BreakerSettings(),
    ): ZLayer[Client, Nothing, MavenCentralRepo] =
      ZLayer.scoped:
        defer:
          val client = ZIO.service[Client].run
          val built = ZIO.foreach(mirrors): mirror =>
            CircuitBreaker.make[Throwable](
              trippingStrategy = TrippingStrategy.failureRate(breaker.failureRate, breaker.window, breaker.minCalls, 10),
              resetPolicy      = Retry.Schedules.exponentialBackoff(resetMin, resetMax),
              isFailure        = trippingError,
            ).map(Live(mirror, _))
          .run
          ZIO.foreachDiscard(built)(logStateChanges).run
          val bucket = TokenBucket.make(rateLimit.perSecond, rateLimit.burst).run
          val limited = Ref.make(0L).run
          Repo(client, built, requestTimeout, rateLimit, bucket, limited)

    private def logStateChanges(live: Live): ZIO[Scope, Nothing, Unit] =
      live.breaker.stateChanges.flatMap: changes =>
        changes.take.flatMap: change =>
          ZIO.logAnnotate(LogAnnotation("mirror", live.mirror.url.encode), LogAnnotation("state", change.to.toString))(
            ZIO.logWarning("Mirror circuit breaker changed state"))
        .forever.forkScoped.unit

    /** A token bucket: `perSecond` tokens refill continuously up to `burst`. */
    private final class TokenBucket(state: Ref[(Double, Long)], perSecond: Double, burst: Int):
      def tryAcquire: UIO[Boolean] =
        ZIO.succeed(java.lang.System.nanoTime()).flatMap: now =>
          state.modify: (tokens, last) =>
            val refilled = math.min(burst.toDouble, tokens + (now - last) / 1e9 * perSecond)
            if refilled >= 1 then (true, (refilled - 1, now)) else (false, (refilled, now))

      def acquireWithin(wait: Duration): UIO[Boolean] =
        tryAcquire.repeat(Schedule.recurWhile[Boolean](!_) && Schedule.spaced(50.millis) && Schedule.upTo(wait)).map(_._1)

    private object TokenBucket:
      def make(perSecond: Double, burst: Int): UIO[TokenBucket] =
        Ref.make((burst.toDouble, java.lang.System.nanoTime())).map(TokenBucket(_, perSecond, burst))

    private final class Repo(
      client: Client,
      mirrors: List[Live],
      requestTimeout: Duration,
      rateLimit: RateLimit,
      bucket: TokenBucket,
      limited: Ref[Long],
    ) extends MavenCentralRepo:

      private val authoritative = mirrors.filter(_.mirror.authoritative)
      private val fallbacks     = mirrors.filterNot(_.mirror.authoritative)

      def rateLimited: UIO[Long] = limited.get

      /** One attempt: the response, or a failure counted against the mirror's breaker. */
      private def attempt[R](live: Live, send: URL => ZIO[R, Throwable, Response], path: Path): ZIO[R, CircuitBreaker.CircuitBreakerCallError[Throwable], (Response, URL)] =
        val url = live.mirror.url.addPath(path)
        live.breaker(
          send(url)
            .timeoutFail(IOException(s"Request to ${url.encode} timed out after ${requestTimeout.render}"))(requestTimeout)
            .flatMap: response =>
              if isFallbackWorthy(response.status) then ZIO.fail(temporaryOrUnknown(response))
              else ZIO.succeed(response -> url)
        )

      private def logged[R](live: Live)(result: ZIO[R, CircuitBreaker.CircuitBreakerCallError[Throwable], (Response, URL)]): ZIO[R, Option[Throwable], (Response, URL)] =
        result.foldZIO(
          {
            case CircuitBreakerOpen =>
              ZIO.logAnnotate(LogAnnotation("mirror", live.mirror.url.encode))(ZIO.logDebug("Circuit open, skipping")) *> ZIO.fail(None)
            case WrappedError(t) =>
              ZIO.logAnnotate(
                LogAnnotation("mirror", live.mirror.url.encode),
                LogAnnotation("errorClass", t.getClass.getSimpleName),
                LogAnnotation("error", Option(t.getMessage).getOrElse("-")),
              )(ZIO.logWarning("Mirror failed")) *> ZIO.fail(Some(t))
          },
          ZIO.succeed(_),
        )

      /** Ask the authoritative mirror, within its budget (waiting up to `wait`). */
      private def askAuthoritative[R](send: URL => ZIO[R, Throwable, Response], path: Path, wait: Duration): ZIO[R, Option[Throwable], (Response, URL)] =
        authoritative.headOption match
          case None => ZIO.fail(None)
          case Some(live) =>
            val permit = if wait <= Duration.Zero then bucket.tryAcquire else bucket.acquireWithin(wait)
            permit.flatMap:
              case true  => logged(live)(attempt(live, send, path))
              case false => limited.update(_ + 1) *> ZIO.fail(None)

      /** Fallback mirrors in order: the first positive answer, or the URL of the first that doesn't have it. */
      private def askFallbacks[R](send: URL => ZIO[R, Throwable, Response], path: Path, remaining: List[Live]): ZIO[R, Option[Throwable], Either[URL, (Response, URL)]] =
        remaining match
          case Nil => ZIO.fail(None)
          case live :: rest =>
            logged(live)(attempt(live, send, path)).foldZIO(
              err => askFallbacks(send, path, rest).mapError(_.orElse(err)),
              (response, url) =>
                if isAbsent(response.status) then askFallbacks(send, path, rest).map(_.left.map(_ => url)).orElseSucceed(Left(url))
                else ZIO.succeed(Right(response -> url)),
            )

      private def run[R](send: URL => ZIO[R, Throwable, Response], path: Path, kind: RequestKind): ZIO[R, Throwable, (Response, URL)] =
        def unavailable(err: Option[Throwable]): Throwable = err.getOrElse(AllMirrorsUnavailable(path))
        // The authoritative mirror, then fallbacks; a fallback's "not found" asks the authoritative one again.
        val authoritativeFirst =
          askAuthoritative(send, path, Duration.Zero).catchAll: firstErr =>
            askFallbacks(send, path, fallbacks).foldZIO(
              err => ZIO.fail(unavailable(firstErr.orElse(err))),
              {
                case Right(ok) => ZIO.succeed(ok)
                case Left(fallbackUrl) =>
                  // Only retry repo1 if it was skipped for budget, not if it just failed.
                  if firstErr.isDefined then ZIO.fail(NotFoundOnFallback(fallbackUrl))
                  else askAuthoritative(send, path, rateLimit.authoritativeWait).mapError(_ => NotFoundOnFallback(fallbackUrl))
              },
            )
        kind match
          case RequestKind.Fresh => authoritativeFirst
          case RequestKind.Listing =>
            // Only HTML-listing mirrors (repo1), within its budget; a GCS bucket listing is `list`'s fallback.
            mirrors.find(_.mirror.listings == Listings.Html) match
              case None => ZIO.fail(AllMirrorsUnavailable(path))
              case Some(lister) =>
                bucket.acquireWithin(rateLimit.authoritativeWait).flatMap:
                  case false => limited.update(_ + 1) *> ZIO.fail(AllMirrorsUnavailable(path))
                  case true  =>
                    logged(lister)(attempt(lister, send, path)).mapError(unavailable).flatMap: (response, url) =>
                      if isAbsent(response.status) && !lister.mirror.authoritative then ZIO.fail(NotFoundOnFallback(url))
                      else ZIO.succeed(response -> url)
          case RequestKind.Immutable =>
            askFallbacks(send, path, fallbacks).foldZIO(
              _ => askAuthoritative(send, path, rateLimit.authoritativeWait).mapError(err => unavailable(err)),
              {
                case Right(ok) => ZIO.succeed(ok)
                case Left(fallbackUrl) =>
                  askAuthoritative(send, path, rateLimit.authoritativeWait).mapError(_ => NotFoundOnFallback(fallbackUrl))
              },
            )

      def request(path: Path, method: Method, headers: Headers, content: Body, kind: RequestKind): ZIO[Any, Throwable, (Response, URL)] =
        run(url => client.batched(Request(method = method, url = url, headers = headers, body = content)), path, kind)

      def streaming(path: Path, method: Method, headers: Headers, kind: RequestKind): ZIO[Scope, Throwable, (Response, URL)] =
        run(url => client(Request(method = method, url = url, headers = headers)), path, kind)

      def list(path: Path, timeout: Duration): ZIO[Any, Throwable, Listed] =
        val dir = path.addTrailingSlash
        request(dir, kind = RequestKind.Listing).flatMap: (response, url) =>
          if isAbsent(response.status) then ZIO.succeed(Listed.Absent(url))
          else if response.status.isSuccess then
            responseToNames(response).map(Listed.Names(_, response.header(Header.LastModified).map(_.value), url))
          else ZIO.fail(temporaryOrUnknown(response))
        .catchSome:
          case e if trippingError.isDefinedAt(e) || e.isInstanceOf[AllMirrorsUnavailable] =>
            bucketListing(path, timeout).someOrFail(e)

      /** The first GCS bucket mirror's listing of `path`, if a mirror has one and answers in time. */
      private def bucketListing(path: Path, timeout: Duration): ZIO[Any, Throwable, Option[Listed]] =
        mirrors.collectFirst { case Live(Mirror(url, _, Listings.GcsBucket(api, bucket, prefix)), _) => (url, api, bucket, prefix) } match
          case None => ZIO.none
          case Some((mirrorUrl, api, bucket, prefix)) =>
            val dirPrefix = prefix + path.dropLeadingSlash.addTrailingSlash.encode.stripPrefix("/")
            def page(token: Option[String]): ZIO[Any, Throwable, (Seq[String], Option[String])] =
              val params = QueryParams(Map(
                "prefix" -> Chunk(dirPrefix), "delimiter" -> Chunk("/"), "maxResults" -> Chunk("1000"),
                "fields" -> Chunk("prefixes,nextPageToken"),
              ) ++ token.map(t => "pageToken" -> Chunk(t)))
              val url = api.addPath(Path.decode(s"$bucket/o")).copy(queryParams = params)
              client.batched(Request.get(url)).flatMap: response =>
                if !response.status.isSuccess then ZIO.fail(temporaryOrUnknown(response))
                else response.body.asString.map(parseBucketPage(dirPrefix))
            def all(token: Option[String], acc: Seq[String]): ZIO[Any, Throwable, Seq[String]] =
              page(token).flatMap: (names, next) =>
                next.fold(ZIO.succeed(acc ++ names))(t => all(Some(t), acc ++ names))
            all(None, Seq.empty)
              .timeoutFail(IOException(s"GCS listing of ${path.encode} took over ${timeout.render}"))(timeout)
              .flatMap: names =>
                val url = mirrorUrl.addPath(path.addTrailingSlash)
                // An empty listing on a fallback isn't proof the directory doesn't exist.
                if names.isEmpty then ZIO.fail(NotFoundOnFallback(url))
                else ZIO.some(Listed.Names(names, None, url))

  /** The subdirectory names in one page of a GCS JSON listing (`{"prefixes":[…],"nextPageToken":…}`). */
  private[zio_mavencentral] def parseBucketPage(dirPrefix: String)(json: String): (Seq[String], Option[String]) =
    import zio.json.*
    import zio.json.ast.Json
    json.fromJson[Json].toOption match
      case Some(Json.Obj(fields)) =>
        val prefixes = fields.collectFirst { case ("prefixes", Json.Arr(ps)) => ps.collect { case Json.Str(p) => p } }.getOrElse(Chunk.empty)
        val token    = fields.collectFirst { case ("nextPageToken", Json.Str(t)) => t }
        (prefixes.map(_.stripPrefix(dirPrefix).stripSuffix("/")).filter(_.nonEmpty).toSeq, token)
      case _ => (Seq.empty, None)

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
  private def versionsAtGroupPath(groupId: GroupId): ZIO[MavenCentralRepo, TemporaryServerError | Throwable, Seq[Version]] =
    defer:
      val path = artifactPath(groupId) / "maven-metadata.xml"
      val (response, _) = Client.requestWithFallback(path, Method.GET).run
      response.status match
        case status if status.isSuccess =>
          val body = response.body.asString.run
          val xml  = scala.xml.XML.loadString(body)
          (xml \ "versioning" \ "versions" \ "version").map(n => Version(n.text)).toSeq
        case status if isAbsent(status) =>
          ZIO.succeed(Seq.empty[Version]).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  // Removes any versions found in this groupId
    // While repo1 can't be asked, GCS's 404 for the group's metadata is unknown, not absent;
    // treat it as "no versions" (a group-as-artifact's versions may then show as artifacts).
    .catchSome:
      case _: NotFoundOnFallback => ZIO.succeed(Seq.empty[Version])

  def searchArtifacts(groupId: GroupId): ZIO[MavenCentralRepo, GroupIdNotFoundError | TemporaryServerError | Throwable, WithCacheInfo[Seq[ArtifactId]]] =
    val listing = ZIO.serviceWithZIO[MavenCentralRepo](_.list(artifactPath(groupId)))
    listing.zipWithPar(versionsAtGroupPath(groupId)):
      case (Listed.Absent(_), _) =>
        ZIO.fail(GroupIdNotFoundError(groupId))
      case (Listed.Names(names, lastModified, url), versions) =>
        ZIO.succeed(WithCacheInfo(names.diff(versions).map(ArtifactId(_)), lastModified, None, Some(url)))
    .flatten


  /**
   * Every published version, newest first by Maven version order ([[Version.ordering]]).
   * `maven-metadata.xml` lists versions in *publish* order, which is not version
   * order: a maintenance release (e.g. 2.4.1.3 after 2.4.2) is appended last, so the
   * file order must not be trusted.
   */
  def searchVersions(groupId: GroupId, artifactId: ArtifactId): ZIO[MavenCentralRepo, GroupIdOrArtifactIdNotFoundError | TemporaryServerError | Throwable, WithCacheInfo[Seq[Version]]] =
    defer:
      val metadata = mavenMetadata(groupId, artifactId).run
      WithCacheInfo(versionsOf(metadata.value), metadata.maybeLastModified, metadata.maybeEtag, metadata.servedBy)

  def isModifiedSince(dateTime: ZonedDateTime, groupId: GroupId, maybeArtifactId: Option[ArtifactId] = None):
    ZIO[MavenCentralRepo, GroupIdNotFoundError | GroupIdOrArtifactIdNotFoundError | Throwable, Boolean] =
      defer:
        val path = maybeArtifactId.fold(artifactPath(groupId).addTrailingSlash)(artifactId => artifactPath(groupId, Some(ArtifactAndVersion(artifactId))) / "maven-metadata.xml")
        val kind = if maybeArtifactId.isEmpty then RequestKind.Listing else RequestKind.Fresh
        val (response, _) = Client.requestWithFallback(path, Method.HEAD, Headers(Header.IfModifiedSince(dateTime)), kind = kind).run
        response.status match
          case Status.Ok =>
            // workaround for MavenCentral exact time matching on file not working correctly
            if response.header(Header.LastModified).map(_.value).contains(dateTime) then
              ZIO.succeed(false).run
            else
              ZIO.succeed(true).run
          case Status.NotModified => ZIO.succeed(false).run
          case status if isAbsent(status) => ZIO.fail(maybeArtifactId.fold(GroupIdNotFoundError(groupId))(GroupIdOrArtifactIdNotFoundError(groupId, _))).run
          case _ => ZIO.fail(temporaryOrUnknown(response)).run

  /** Distinct versions sorted newest first by [[Version.ordering]]. */
  def newestFirst(versions: Seq[Version]): Seq[Version] =
    versions.distinct.sorted(using Version.ordering.reverse)

  /**
   * The latest version from a list: the highest release, or — when
   * [[includePreReleases]] is true, or the artifact has only pre-releases — the
   * highest version of any kind.
   */
  def latestOf(versions: Seq[Version], includePreReleases: Boolean = false): Option[Version] =
    val sorted = newestFirst(versions)
    if includePreReleases then sorted.headOption
    else sorted.find(v => !Version.isPreRelease(v)).orElse(sorted.headOption)

  /**
   * The latest version of an artifact. By default this is the highest *release*
   * (so Spring AI resolves to 2.0.1, not 2.1.0-M1, and Netty to 4.2.x, not
   * 5.0.0.Alpha2); pass `includePreReleases = true` for the highest version of any
   * kind. An artifact that has only pre-releases resolves to its highest one.
   */
  def latest(groupId: GroupId, artifactId: ArtifactId, includePreReleases: Boolean = false): ZIO[MavenCentralRepo, GroupIdOrArtifactIdNotFoundError | TemporaryServerError | Throwable, Option[Version]] =
    searchVersions(groupId, artifactId).map(v => latestOf(v.value, includePreReleases))

  /**
   * Resolve the latest published version for a `GroupArtifact`, with a
   * typed failure channel. Equivalent to:
   *   `latest(g, a, includePreReleases).someOrFail(LatestNotFound(ga))`
   * with upstream failures typed as [[LatestUnavailable]] and any other
   * throwable `orDie`'d.
   *
   * Transient upstream failures (5xx / 429 / 403, network errors) are
   * handled by [[MavenCentralRepo]]'s mirror fallback + circuit breaker,
   * not by per-call retries; when every mirror fails, the result is
   * [[LatestUnavailable]].
   */
  def latestOrFail(groupArtifact: GroupArtifact, includePreReleases: Boolean = false):
      ZIO[MavenCentralRepo, GroupIdOrArtifactIdNotFoundError | LatestNotFound | LatestUnavailable, Version] =
    latest(groupArtifact.groupId, groupArtifact.artifactId, includePreReleases)
      .catchAll:
        case e: GroupIdOrArtifactIdNotFoundError                => ZIO.fail(e)
        case t: Throwable if JarCache.isUpstreamFailure(t)      => ZIO.fail(LatestUnavailable(groupArtifact, JarCache.describeUpstreamFailure(t)))
        case t: Throwable                                       => ZIO.die(t)
      .someOrFail(LatestNotFound(groupArtifact))

  def isArtifact(groupId: GroupId, artifactId: ArtifactId): ZIO[MavenCentralRepo, Throwable, Boolean] =
    defer:
      val metadata = mavenMetadata(groupId, artifactId).run
      val body = metadata.value.toString
      // checks that the maven-metadata.xml contains the groupId
      body.contains(s"<groupId>$groupId</groupId>")
    .catchAll:
      _ => ZIO.succeed(false)

  def artifactExists(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[MavenCentralRepo, Throwable, Boolean] =
    defer:
      // The POM, not the version directory: every mirror has POMs, only repo1 has directory pages.
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version.pom"
      val (response, _) = Client.requestWithFallback(path, Method.HEAD, kind = RequestKind.Immutable).run
      response.status.isSuccess

  def pom(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[MavenCentralRepo, NotFoundError | TemporaryServerError | Throwable, Elem] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / s"$artifactId-$version.pom"
      val (response, url) = Client.requestWithFallback(path, Method.GET, kind = RequestKind.Immutable).run
      response.status match
        case status if status.isSuccess =>
          val body = response.body.asString.run
          scala.xml.XML.loadString(body)
        case status if isAbsent(status) =>
          ZIO.fail(NotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  def mavenMetadata(groupId: GroupId, artifactId: ArtifactId): ZIO[MavenCentralRepo, GroupIdOrArtifactIdNotFoundError | TemporaryServerError | Throwable, WithCacheInfo[Elem]] =
    mavenMetadataIfModified(groupId, artifactId, None, None).someOrElseZIO(ZIO.dieMessage("304 without validators"))

  /**
   * `maven-metadata.xml`, unless it is unchanged since a copy with these validators:
   * `None` for a `304 Not Modified`. A cache revalidates with this, so an unchanged
   * artifact costs a 304 instead of the whole file.
   */
  def mavenMetadataIfModified(groupId: GroupId, artifactId: ArtifactId, etag: Option[Header.ETag], lastModified: Option[ZonedDateTime]):
      ZIO[MavenCentralRepo, GroupIdOrArtifactIdNotFoundError | TemporaryServerError | Throwable, Option[WithCacheInfo[Elem]]] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId))) / "maven-metadata.xml"
      val validators = Headers(
        etag.map(e => Header.Custom(Header.IfNoneMatch.name, e.renderedValue)).toList ++ lastModified.map(Header.IfModifiedSince(_)).toList
      )
      val (response, url) = Client.requestWithFallback(path, Method.GET, validators).run
      response.status match
        case Status.NotModified =>
          None
        case status if status.isSuccess =>
          val body = response.body.asString.run
          Some(WithCacheInfo(
            scala.xml.XML.loadString(body),
            response.header(Header.LastModified).map(_.value),
            response.header(Header.ETag),
            Some(url),
          ))
        case status if isAbsent(status) =>
          ZIO.fail(GroupIdOrArtifactIdNotFoundError(groupId, artifactId)).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  /** The versions listed in a `maven-metadata.xml`, newest first. */
  def versionsOf(metadata: Elem): Seq[Version] =
    newestFirst((metadata \ "versioning" \ "versions" \ "version").map(n => Version(n.text)))

  private def artifactUrl(groupId: GroupId, artifactId: ArtifactId, version: Version)(filename: String): ZIO[MavenCentralRepo, NotFoundError | TemporaryServerError | Throwable, URL] =
    defer:
      val path = artifactPath(groupId, Some(ArtifactAndVersion(artifactId, Some(version)))) / filename
      val (response, url) = Client.requestWithFallback(path, Method.HEAD, kind = RequestKind.Immutable).run
      response.status match
        case status if status.isSuccess =>
          ZIO.succeed(url).run
        case status if isAbsent(status) =>
          ZIO.fail(NotFoundError(groupId, artifactId, version)).run
        case _ =>
          ZIO.fail(temporaryOrUnknown(response)).run

  /** URL to the main `.jar` for a GAV. */
  def jarUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[MavenCentralRepo, NotFoundError | TemporaryServerError | Throwable, URL] =
    artifactUrl(groupId, artifactId, version)(s"$artifactId-$version.jar")

  def javadocUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[MavenCentralRepo, NotFoundError | TemporaryServerError | Throwable, URL] =
    artifactUrl(groupId, artifactId, version)(s"$artifactId-$version-javadoc.jar")

  def sourcesUri(groupId: GroupId, artifactId: ArtifactId, version: Version): ZIO[MavenCentralRepo, NotFoundError | TemporaryServerError | Throwable, URL] =
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
   * Streams the response body to disk via `ZSink.fromPath`, with an
   * intermediate `ZPipeline.rechunk(64 KB)` to bound chunk size before
   * the sink sees them.
   *
   * Why the rechunk is necessary
   * ----------------------------
   * `zio-http`'s `AsyncBodyReader` sits in `Buffering` state between
   * `Client.streaming` returning (headers received) and user code
   * materializing `response.body.asStream` (which calls `connect`
   * under the hood). While buffering, every `HttpContent` frame is
   * appended to an internal `mutable.ArrayBuilder.ofByte`, and
   * `ctx.read()` is called to pull more — the buffer grows unbounded.
   *
   * On `connect`, the entire accumulated buffer is flushed as a
   * **single** `Chunk.ByteArray`. For a multi-MB jar arriving while
   * ZIO is still doing scope/queue/fiber-scheduling setup between
   * `Client.streaming` and `.run(sink)`, that single Chunk can be
   * tens of MB. `ZSink.fromPath` then calls `byteChunk.toArray`
   * (allocating a fresh `byte[]` of the chunk's full size) before
   * the `FileChannel.write` — which on a memory-tight deploy is the
   * difference between OOM and not.
   *
   * `ZPipeline.rechunk(N)` slices any incoming chunk into ≤ N-byte
   * chunks before the sink sees them. The slicing is O(1) per slice
   * (Chunk slices are views over the same backing array), so it
   * doesn't introduce extra copies; it just stops `Chunk.toArray`
   * from being asked to allocate more than 64 KB at a time. The
   * giant upstream Chunk still exists briefly until the pipeline
   * drains it, but the doubled-allocation that was tipping us over
   * is gone.
   *
   *  - 5xx / 429 / 403 responses fail with `TemporaryServerError`.
   *  - Other non-success responses fail with an internal `UnknownError`
   *    surfaced as a generic `Throwable`.
   *
   * Note: `downloadJar` resolves `source` directly without going through
   * the [[MavenCentralRepo]] mirror fallback. Callers wanting mirror
   * fallback should obtain the URL via [[jarUri]] / [[javadocUri]] /
   * [[sourcesUri]] (which do go through the repo) and only then call
   * `downloadJar` on the resulting URL.
   *
   * `responseTimeout` bounds how long we wait for the response *headers*
   * (i.e. `Client.streaming` to return) — the phase where a stalled or
   * throttled upstream connection would otherwise hang the fiber forever.
   * It deliberately does not bound the body transfer itself, so a large
   * but steadily-progressing download isn't cut off; a timeout here fails
   * with an `IOException`.
   */
  def downloadJar(source: URL, target: File, responseTimeout: Duration = 60.seconds): ZIO[Client, Throwable, JarMeta] =
    ZIO.scoped:
      defer:
        val response =
          Client.streaming(Request.get(source))
            .timeoutFail(IOException(s"Request to $source timed out after ${responseTimeout.render}"))(responseTimeout)
            .run
        response.status match
          case status if status.isError => ZIO.fail(temporaryOrUnknown(response)).run
          case _ => // success, continue
        response.body.asStream
          .via(ZPipeline.rechunk(64 * 1024))
          .run(ZSink.fromPath(target.toPath))
          .run
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


    type UploadStream = ZStream[Any, Throwable, Byte]

    private[zio_mavencentral] def uploadBody(
      filename: String,
      zip: UploadStream,
      boundary: Boundary,
    ): Body =
      val safeFilename = filename
        .replace("\\", "%5C")
        .replace("\"", "%22")
        .replace("\r", "")
        .replace("\n", "")
      val prefix = Chunk.fromArray(
        (s"--${boundary.id}\r\n" +
          s"Content-Disposition: form-data; name=\"bundle\"; filename=\"$safeFilename\"\r\n" +
          "Content-Type: application/octet-stream\r\n" +
          "\r\n").getBytes(StandardCharsets.UTF_8),
      )
      val suffix = Chunk.fromArray(s"\r\n--${boundary.id}--\r\n".getBytes(StandardCharsets.UTF_8))
      Body.fromStreamChunked(ZStream.fromChunk(prefix) ++ zip ++ ZStream.fromChunk(suffix))
        .contentType(MediaType.multipart.`form-data`, boundary)

    def upload(filename: String, zip: Array[Byte]): ZIO[Sonatype, Throwable, DeploymentId] =
      upload(filename, ZStream.fromChunk(Chunk.fromArray(zip)))

    def upload(filename: String, zip: UploadStream): ZIO[Sonatype, Throwable, DeploymentId] =
      ZIO.serviceWithZIO[Sonatype]:
        sonatype =>
          ZIO.scoped:
            defer:
              val boundary = Boundary.randomUUID.run
              val body = uploadBody(filename, zip, boundary)
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
      uploadVerifyAndPublish(filename, ZStream.fromChunk(Chunk.fromArray(zip)))

    def uploadVerifyAndPublish(filename: String, zip: UploadStream): ZIO[Sonatype, Throwable, Unit] =
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

    /** Fresh incremental signer for one payload. The default preserves source
      * compatibility for custom Signer implementations that only support the
      * original whole-chunk API. */
    def newSession: IO[Throwable, Signer.Session] =
      ZIO.fail(UnsupportedOperationException("incremental signing is not supported by this Signer"))

  object Signer:
    trait Session:
      def update(bytes: Chunk[Byte]): IO[Throwable, Unit]
      def finish: IO[Throwable, Chunk[Byte]]

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

    private def signatureGenerator: IO[Throwable, PGPSignatureGenerator] =
      ZIO.attempt:
        val signerBuilder = JcaPGPContentSignerBuilder(publicKey.getAlgorithm, HashAlgorithmTags.SHA256)
        val generator = PGPSignatureGenerator(signerBuilder, publicKey)
        generator.init(PGPSignature.BINARY_DOCUMENT, privKey)
        generator

    private def finishSignature(generator: PGPSignatureGenerator): IO[Throwable, Chunk[Byte]] =
      // The result chunk MUST be read after the streams are closed:
      // ArmoredOutputStream writes the trailing CRC and end-of-armor marker on
      // close(), and BCPGOutputStream may buffer partial packet data.
      ZIO.attempt(ChunkBuilder.make[Byte]()).flatMap: builder =>
        ZIO.scoped:
          defer:
            val rawOut = ZIO.fromAutoCloseable(ZIO.attempt(ChunkBuilderOutputStream(builder))).run
            val armor = ZIO.fromAutoCloseable(ZIO.attempt(ArmoredOutputStream(rawOut))).run
            val bOut = ZIO.fromAutoCloseable(ZIO.attempt(BCPGOutputStream(armor))).run
            ZIO.attempt(generator.generate().encode(bOut)).run
        .as(builder.result())

    override def newSession: IO[Throwable, Signer.Session] =
      for
        generator <- signatureGenerator
        finished  <- Ref.make(false)
      yield new Signer.Session:
        def update(bytes: Chunk[Byte]): IO[Throwable, Unit] =
          finished.get.flatMap: isFinished =>
            ZIO.fail(IllegalStateException("signing session is already finished")).when(isFinished) *>
              ZIO.attempt(bytes.foreach(generator.update))

        def finish: IO[Throwable, Chunk[Byte]] =
          finished.getAndSet(true).flatMap: wasFinished =>
            ZIO.fail(IllegalStateException("signing session is already finished")).when(wasFinished) *>
              finishSignature(generator)

    override def ascSign(toSign: Chunk[Byte]): IO[Throwable, Option[Chunk[Byte]]] =
      (for
        session   <- newSession
        _         <- session.update(toSign)
        signature <- session.finish
      yield Some(signature)).mapError(signError)

/**
 * Ambient `Schema` instances for the Maven Central coordinate types.
 *
 * Defined outside `object MavenCentral` so the opaque `GroupId` / `ArtifactId`
 * / `Version` types are opaque here — the primitive transforms are genuinely
 * `Schema[GroupId]` (not `Schema[String]`), and `GroupArtifact` /
 * `GroupArtifactVersion` derive as *records* carrying their field
 * `@description`s (deriving in the same compilation unit as the annotated case
 * classes keeps the annotations readable). The record schemas are re-exported
 * as ambient givens via the case classes' companions; the opaque-type givens
 * here can be imported (`import MavenCentralSchemas.given`) by code that
 * derives other records over these types.
 */
object MavenCentralSchemas:
  import zio.schema.DeriveSchema
  import MavenCentral.*

  given Schema[GroupId]    = Schema.primitive[String].transform(GroupId(_), _.toString)
  given Schema[ArtifactId] = Schema.primitive[String].transform(ArtifactId(_), _.toString)
  given Schema[Version]    = Schema.primitive[String].transform(Version(_), _.toString)

  val groupArtifact: Schema[GroupArtifact] = DeriveSchema.gen[GroupArtifact]
  val groupArtifactVersion: Schema[GroupArtifactVersion] = DeriveSchema.gen[GroupArtifactVersion]
