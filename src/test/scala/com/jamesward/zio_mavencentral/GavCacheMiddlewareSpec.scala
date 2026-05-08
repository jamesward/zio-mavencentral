package com.jamesward.zio_mavencentral

import zio.*
import zio.direct.*
import zio.http.*
import zio.test.*

object GavCacheMiddlewareSpec extends ZIOSpecDefault:

  given CanEqual[Status, Status] = CanEqual.derived

  private def req(method: Method, path: String): Request =
    Request(method = method, url = URL.decode("http://localhost" + path).toOption.get)

  private val sampleResponse = Response.text("hello").status(Status.Ok)

  def spec = suite("GavCacheMiddleware")(

    suite("gavFromPath")(
      test("parses a 3-segment path") {
        val gav = GavCacheMiddleware.gavFromPath(Path.empty / "g" / "a" / "1")
        assertTrue(
          gav.exists(_.toString == "g/a/1"),
        )
      },
      test("parses when there are extra trailing segments") {
        val gav = GavCacheMiddleware.gavFromPath(Path.empty / "org.foo" / "bar" / "1.2.3" / "subdir" / "Baz.html")
        assertTrue(
          gav.exists(_.toString == "org.foo/bar/1.2.3"),
        )
      },
      test("returns None when path is too short") {
        assertTrue(
          GavCacheMiddleware.gavFromPath(Path.empty / "only-one").isEmpty,
          GavCacheMiddleware.gavFromPath(Path.empty / "two" / "segments").isEmpty,
        )
      },
    ),

    suite("isImmutableAssetPath")(
      test("true for GET on a 4+ segment GAV path with concrete version") {
        assertTrue(GavCacheMiddleware.isImmutableAssetPath(req(Method.GET, "/g/a/1.0/file.html")))
      },
      test("true for HEAD on a GAV path") {
        assertTrue(GavCacheMiddleware.isImmutableAssetPath(req(Method.HEAD, "/g/a/1.0/file.html")))
      },
      test("false for the special `latest` version") {
        assertTrue(!GavCacheMiddleware.isImmutableAssetPath(req(Method.GET, "/g/a/latest/file.html")))
      },
      test("false for non-GET/HEAD methods") {
        assertTrue(
          !GavCacheMiddleware.isImmutableAssetPath(req(Method.POST, "/g/a/1.0/file.html")),
          !GavCacheMiddleware.isImmutableAssetPath(req(Method.PUT, "/g/a/1.0/file.html")),
        )
      },
      test("false when path has fewer than 4 segments") {
        // 3-segment path is the GAV root with no asset suffix; the middleware
        // is only meant to stamp the asset responses themselves.
        assertTrue(!GavCacheMiddleware.isImmutableAssetPath(req(Method.GET, "/g/a/1.0")))
      },
    ),

    suite("cacheHeaders aspect")(
      test("stamps stable validators + cache-control on a GAV asset response") {
        val handler  = Handler.succeed(sampleResponse)
        val withAspect = handler @@ GavCacheMiddleware.cacheHeaders()
        defer:
          val r = ZIO.scoped(withAspect.runZIO(req(Method.GET, "/g/a/1.0/file.html"))).merge.run
          val etag = r.header(Header.ETag)
          val lm   = r.header(Header.LastModified)
          val cc   = r.header(Header.CacheControl)
          assertTrue(
            r.status == Status.Ok,
            etag.exists(_.renderedValue.contains("/g/a/1.0/file.html")),
            // Epoch is the pinned Last-Modified
            lm.exists(_.renderedValue.contains("1970")),
            cc.isDefined,
          )
      },
      test("leaves non-GAV paths untouched (path too short)") {
        val response = Response.text("hi").status(Status.Ok).addHeader(Header.ETag.Strong("orig"))
        val handler  = Handler.succeed(response)
        val withAspect = handler @@ GavCacheMiddleware.cacheHeaders()
        defer:
          val r = ZIO.scoped(withAspect.runZIO(req(Method.GET, "/groupId"))).merge.run
          assertTrue(
            r.header(Header.ETag).exists(_.renderedValue == "\"orig\""),
            r.header(Header.CacheControl).isEmpty,
          )
      },
      test("leaves /latest paths untouched") {
        val handler  = Handler.succeed(sampleResponse)
        val withAspect = handler @@ GavCacheMiddleware.cacheHeaders()
        defer:
          val r = ZIO.scoped(withAspect.runZIO(req(Method.GET, "/g/a/latest/file.html"))).merge.run
          assertTrue(r.header(Header.CacheControl).isEmpty)
      },
    ),

    suite("notModified aspect")(
      test("returns 304 when If-None-Match is set on a GAV path") {
        val handler  = Handler.succeed(sampleResponse)
        val withAspect = handler @@ GavCacheMiddleware.notModified()
        defer:
          val request = req(Method.GET, "/g/a/1.0/file.html")
            .addHeader(Header.IfNoneMatch.Any)
          val r = ZIO.scoped(withAspect.runZIO(request)).merge.run
          assertTrue(
            r.status == Status.NotModified,
            r.header(Header.ETag).isDefined,
          )
      },
      test("returns 304 when If-Modified-Since is set on a GAV path") {
        val handler  = Handler.succeed(sampleResponse)
        val withAspect = handler @@ GavCacheMiddleware.notModified()
        defer:
          val request = req(Method.GET, "/g/a/1.0/file.html")
            .addHeader(Header.IfModifiedSince(java.time.ZonedDateTime.now()))
          val r = ZIO.scoped(withAspect.runZIO(request)).merge.run
          assertTrue(r.status == Status.NotModified)
      },
      test("falls through to the handler when no validator is present") {
        val handler  = Handler.succeed(sampleResponse)
        val withAspect = handler @@ GavCacheMiddleware.notModified()
        defer:
          val r = ZIO.scoped(withAspect.runZIO(req(Method.GET, "/g/a/1.0/file.html"))).merge.run
          assertTrue(r.status == Status.Ok)
      },
      test("falls through for /latest paths even with a validator") {
        val handler  = Handler.succeed(sampleResponse)
        val withAspect = handler @@ GavCacheMiddleware.notModified()
        defer:
          val request = req(Method.GET, "/g/a/latest/file.html").addHeader(Header.IfNoneMatch.Any)
          val r = ZIO.scoped(withAspect.runZIO(request)).merge.run
          assertTrue(r.status == Status.Ok)
      },
    ),

  )
