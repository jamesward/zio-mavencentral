# AGENTS.md — zio-mavencentral

`zio-mavencentral` is a published Scala 3 / ZIO 2 library (`com.jamesward:zio-mavencentral_3`) for
working with Maven Central: the typed read API (`MavenCentral`), jar/zip downloads, the on-disk
`JarCache`, `GavCacheMiddleware` for zio-http, GAV `PathCodec`s, and the Sonatype Central Portal
publishing client (`MavenCentral.Deploy.Sonatype`). See `README.md` for the feature overview.
It is a library: releases go through sbt-ci-release with `versionScheme := Some("semver-spec")`.

## Skills

- Skills come from the `% Skills` dependency in `build.sbt` (`com.jamesward:skills`) and are
  extracted into `.kiro/skills/` (gitignored, regenerated). Extract them with the `sbt-task` tool
  command `extractSkillsJars` (fallback: `./sbt extractSkillsJars`) after cloning and whenever the
  pinned Skills version changes, then read the relevant `SKILL.md` files before working:
  - `zen-of-scala` — Scala 3 / ZIO idioms for all code and review.
  - `zen-of-james` — design, domain modeling, effects, and testing principles.
  - `zen-of-projects` — project/build conventions (sbt 2, sbt-mcp, SkillsJars, Java 21, CI).
- Don't copy Skill rules into this file; it holds only project-specific guidance.

## Build, Test & Dev Workflow

- Use the MCP server named `sbt-mcp-zio-mavencentral` (`http://127.0.0.1:5121/`, configured in
  `.kiro/settings/mcp.json`) for ALL sbt interactions. Run commands/tasks through its `sbt-task`
  tool; use `list-tasks` to discover tasks/settings or get per-task help. Separate multiple sbt
  commands with `;`. The server only runs while a long-lived sbt session is loaded (for example
  `./sbt` in a terminal); reconnect the MCP client after starting it.
- If the MCP server is unavailable, say so explicitly and fall back to the `./sbt` launcher.
- After editing Scala sources, use the `check` tool of `sbt-mcp-zio-mavencentral` for a fast
  type check (`"scope":"module"` after API changes), then run `compile`/`testFull` via `sbt-task`
  before declaring work done.
- Use `sbt-mcp-zio-mavencentral` for Scala/classpath symbol work: `glob-search`, `inspect`, and
  `symbol-location`. Prefer them over text search or guessing APIs. JavaDoc/ScalaDoc lookups are
  available through the same server's proxied javadocs.dev tools.
- Commands (CLI fallback form):
  - `./sbt "Test / compile"` — compile main + tests with `-language:strictEquality -deprecation -Werror`.
  - `./sbt testFull` — full test suite (what CI runs). `./sbt test` runs sbt 2's incremental selection.
  - `./sbt "testOnly *MavenCentralSpec -- -t downloadAndExtractZip"` — a single test.
- Use Java 21 (CI uses Temurin 21). `--sun-misc-unsafe-memory-access=allow` is added to forked
  test JVMs only when running on JDK 24+.
- Most tests hit the real (free) Maven Central over the network. The Sonatype `upload and verify`
  test publishes for real and only runs when `OSS_DEPLOY_USERNAME` and `OSS_DEPLOY_PASSWORD` are
  set — do not set them unless explicitly asked.
- sbt 2 runs a persistent daemon: after changing env vars or JVM `-D` properties run
  `./sbt shutdown` before the next build, and run it when you are done so no daemon (or the MCP
  port) is left running.

## Releasing

- Run the `Tag Release` workflow (or push a `v<major>.<minor>.<patch>` tag, see `DEV.md`);
  `release.yml` runs `testFull` then `ci-release`.
