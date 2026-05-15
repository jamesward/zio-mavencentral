zio-mavencentral
--------------------

[![javadocs.dev](https://www.javadocs.dev/com.jamesward/zio-mavencentral_3/badge.svg)](https://www.javadocs.dev/com.jamesward/zio-mavencentral_3/latest)

A ZIO 2 / Scala 3 toolkit for working with Maven Central:

- **Read API** (`MavenCentral`) — typed `GroupId` / `ArtifactId` / `Version` / `GroupArtifact[Version]` coordinates, `searchArtifacts`, `searchVersions`, `latest` / `latestOrFail`, `pom`, `mavenMetadata`, `isArtifact`, `artifactExists`, `isModifiedSince`, and `jarUri` / `javadocUri` / `sourcesUri` URL resolution. Requests fall back from `repo1.maven.org` to `repo.maven.apache.org` and can opt in to retry-on-5xx via the `retryOnServerError` extension.
- **Downloads** — stream a jar to a `File` with `downloadJar`, or download-and-extract a zip with `downloadAndExtractZip`, both surfacing `Last-Modified` / `ETag` cache metadata.
- **JarCache** — scoped, on-disk jar cache with random-access `ZipFile` reads, single-flight deduplication of concurrent fetches for the same GAV, and finalizer-driven handle cleanup; entry lookups don't decompress the rest of the jar.
- **GavCacheMiddleware** — `zio-http` `HandlerAspect`s that stamp GAV-versioned routes with a path-derived stable `ETag`, pinned `Last-Modified`, and `Cache-Control: public, max-age, immutable`, and short-circuit conditional GETs to `304 Not Modified` before the upstream fetch.
- **Path codecs** (`MavenCentral.Codecs`) — `zio-http` `PathCodec`s for GAV value types.
- **Publishing** (`MavenCentral.Deploy.Sonatype`) — Sonatype Central Portal client: bundle `upload`, `checkStatus`, `drop`, and an end-to-end `uploadVerifyAndPublish` that polls until the deployment is `VALIDATED` then publishes. `Sonatype.Live` reads `OSS_DEPLOY_USERNAME` / `OSS_DEPLOY_PASSWORD`; `Sonatype.fromCredentials` accepts explicit values.
