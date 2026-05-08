package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.{ArtifactId, GroupArtifactVersion, GroupId, Version}
import zio.*
import zio.http.*

/**
 * HTTP cache machinery for `GET /<groupId>/<artifactId>/<version>/...` style
 * paths where a concrete (non-`latest`) version makes the bytes immutable.
 *
 * Released artifacts on Maven Central never change at a given GAV, so any
 * server that proxies their contents at GAV-shaped URLs can:
 *
 *   1. Return long-lived `public, max-age, immutable` cache headers.
 *   2. Use a *path-derived* `ETag` and a fixed `Last-Modified` so that
 *      validators stay stable across server restarts (jar-fetch ETag /
 *      Last-Modified rotate every redownload, defeating client caches).
 *   3. Short-circuit conditional GETs to `304 Not Modified` *before*
 *      performing the jar fetch — the request is satisfiable from the
 *      ETag/Last-Modified alone.
 *
 * Use:
 * {{{
 *   val routes: Routes[R, Response] = ???
 *   val app = (GavCacheMiddleware.notModified() ++ GavCacheMiddleware.cacheHeaders())
 *               .applyToRoutes(routes)
 * }}}
 *
 * Or apply the two aspects individually with `@@` if you only want one.
 */
object GavCacheMiddleware:

  // Required because the zio-mavencentral build sets `-language:strictEquality`.
  private given CanEqual[Method, Method] = CanEqual.derived

  /** Parse a `Path` into an Optional GAV from its first three segments. */
  def gavFromPath(path: Path): Option[GroupArtifactVersion] =
    val segments = path.segments.toList
    if segments.length >= 3 then
      for
        g <- GroupId.unapply(segments(0))
        a <- ArtifactId.unapply(segments(1))
        v <- Version.unapply(segments(2))
      yield GroupArtifactVersion(g, a, v)
    else None

  /**
   * True if the request is a GET / HEAD whose path begins with
   * `/<groupId>/<artifactId>/<version>/...` and the version is *not* the
   * special `latest` marker. The `latest` exclusion is important because
   * `/.../latest/...` redirects to a changing concrete version, so its
   * response is *not* immutable.
   */
  def isImmutableAssetPath(request: Request): Boolean =
    val segs = request.path.segments.toList
    (request.method == Method.GET || request.method == Method.HEAD)
      && segs.length >= 4
      && segs(2) != "latest"
      && Version.unapply(segs(2)).isDefined
      && GroupId.unapply(segs(0)).isDefined
      && ArtifactId.unapply(segs(1)).isDefined

  /**
   * Path-derived weak `ETag`. Stable across server restarts — depends only
   * on the URL, not on the underlying jar's bytes — which is correct for
   * GAV-versioned content because the bytes themselves are immutable.
   */
  def stableEtag(request: Request): Header.ETag =
    Header.ETag.Weak(request.path.toString)

  /**
   * Pinned `Last-Modified: 1970-01-01T00:00Z` header. Stable across server
   * restarts so `If-Modified-Since` consistently matches.
   */
  val epochLastModified: Header.LastModified =
    Header.LastModified(java.time.ZonedDateTime.ofInstant(java.time.Instant.EPOCH, java.time.ZoneOffset.UTC))

  /** `Cache-Control: public, max-age=<seconds>, immutable`. */
  def immutableCacheControl(maxAge: Duration = 365.days): Header.CacheControl =
    Header.CacheControl.Multiple(NonEmptyChunk(
      Header.CacheControl.Public,
      Header.CacheControl.MaxAge(maxAge.toSeconds.toInt),
      Header.CacheControl.Immutable,
    ))

  /**
   * Outgoing aspect: on successful responses for GAV-immutable paths, strip
   * any disk- or jar-derived `ETag` / `Last-Modified` (they're unstable
   * across restarts) and replace them with the path-derived stable
   * validators plus the immutable cache-control header.
   */
  def cacheHeaders(maxAge: Duration = 365.days): HandlerAspect[Any, Unit] =
    val cc = immutableCacheControl(maxAge)
    HandlerAspect.intercept: (request, response) =>
      if isImmutableAssetPath(request) && response.status.isSuccess then
        response
          .removeHeader(Header.ETag)
          .removeHeader(Header.LastModified)
          .addHeader(stableEtag(request))
          .addHeader(epochLastModified)
          .addHeader(cc)
      else
        response

  /**
   * Incoming aspect: if the client sent `If-None-Match` or
   * `If-Modified-Since` for a GAV-immutable path, short-circuit with
   * `304 Not Modified` *before* routing — any matching validator is
   * sufficient because the bytes are immutable. Skips the jar fetch
   * entirely for cached-client requests.
   */
  def notModified(maxAge: Duration = 365.days): HandlerAspect[Any, Unit] =
    val cc = immutableCacheControl(maxAge)
    HandlerAspect.interceptIncomingHandler:
      Handler.fromFunctionZIO[Request]: request =>
        val hasValidator =
          request.header(Header.IfNoneMatch).isDefined
            || request.header(Header.IfModifiedSince).isDefined
        if isImmutableAssetPath(request) && hasValidator then
          ZIO.fail(
            Response
              .status(Status.NotModified)
              .addHeader(stableEtag(request))
              .addHeader(epochLastModified)
              .addHeader(cc)
          )
        else
          ZIO.succeed((request, ()))
