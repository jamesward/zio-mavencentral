package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.Deploy
import zio.*
import zio.http.{Boundary, Client, MediaType, Request, URL}
import zio.stream.ZStream
import zio.test.*

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

object StreamingUploadSpec extends ZIOSpecDefault:


  private final case class ServerProbe(
    url: URL,
    prefixSeen: CountDownLatch,
    transferEncoding: AtomicReference[String],
    contentLength: AtomicReference[String],
  )

  private def serverProbe: ZIO[Scope, Throwable, ServerProbe] =
    for
      prefixSeen       <- ZIO.succeed(CountDownLatch(1))
      transferEncoding <- ZIO.succeed(AtomicReference[String]())
      contentLength    <- ZIO.succeed(AtomicReference[String]())
      server <- ZIO.acquireRelease(
        ZIO.attemptBlocking(HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0))
      )(server => ZIO.attemptBlocking(server.stop(0)).ignoreLogged)
      _ <- ZIO.attemptBlocking:
        server.createContext("/upload", exchange =>
          val headers = exchange.getRequestHeaders
          transferEncoding.set(headers.getFirst("Transfer-encoding"))
          contentLength.set(headers.getFirst("Content-length"))
          val input = exchange.getRequestBody
          try
            if input.read() >= 0 then prefixSeen.countDown()
            val buffer = Array.ofDim[Byte](8192)
            while input.read(buffer) >= 0 do ()
          finally input.close()
          val response = "deployment-id".getBytes(StandardCharsets.UTF_8)
          exchange.sendResponseHeaders(200, response.length.toLong)
          val output = exchange.getResponseBody
          try output.write(response)
          finally output.close()
        )
        server.start()
      url <- ZIO.fromEither(URL.decode(s"http://127.0.0.1:${server.getAddress.getPort}/upload"))
    yield ServerProbe(url, prefixSeen, transferEncoding, contentLength)
  private val boundary = Boundary("test-boundary")

  def spec = suite("streaming upload")(
    test("multipart body stays lazy and streams the bundle bytes"):
      for
        pulls <- Ref.make(0)
        payload = Chunk.fromArray("bundle-data".getBytes(StandardCharsets.UTF_8))
        stream = ZStream.fromZIO(pulls.update(_ + 1)).drain ++ ZStream.fromChunk(payload)
        body = Deploy.uploadBody("bundle.zip", stream, boundary)
        before <- pulls.get
        encoded <- body.asStream.runCollect
        after <- pulls.get
        text = String(encoded.toArray, StandardCharsets.UTF_8)
      yield assertTrue(
        before == 0,
        after == 1,
        body.knownContentLength.isEmpty,
        body.materializedContent.isEmpty,
        body.mediaType.contains(MediaType.multipart.`form-data`),
        text.startsWith("--test-boundary\r\n"),
        text.contains("Content-Disposition: form-data; name=\"bundle\"; filename=\"bundle.zip\"\r\n"),
        text.contains("\r\n\r\nbundle-data\r\n"),
        text.endsWith("--test-boundary--\r\n"),
      )
    ,
    test("stream failures remain in the typed error channel"):
      val expected = RuntimeException("stream failed")
      val body = Deploy.uploadBody("bundle.zip", ZStream.fail(expected), boundary)
      body.asStream.runCollect.either.map:
        case Left(actual) => assertTrue(actual eq expected)
        case Right(_)     => assertTrue(false)
    ,
    test("zio-http sends multipart bytes before pulling the gated bundle stream"):
      ZIO.scoped:
        for
          probe <- serverProbe
          pulls <- Ref.make(0)
          awaitPrefix = ZIO.attemptBlocking(probe.prefixSeen.await(10, TimeUnit.SECONDS))
            .filterOrFail(identity)(RuntimeException("request body was buffered before transmission"))
          bundle =
            ZStream.fromZIO(pulls.update(_ + 1) *> awaitPrefix).drain ++
              ZStream.fromChunk(Chunk.fromArray("payload".getBytes(StandardCharsets.UTF_8)))
          body = Deploy.uploadBody("bundle.zip", bundle, boundary)
          response <- Client.streaming(Request.post(probe.url, body))
          responseText <- response.body.asString
          pullCount <- pulls.get
        yield assertTrue(
          responseText == "deployment-id",
          pullCount == 1,
          Option(probe.transferEncoding.get()).exists(_.equalsIgnoreCase("chunked")),
          Option(probe.contentLength.get()).isEmpty,
        )
      .provide(Client.default)
  )
