# AGENTS.md — zio-mavencentral

`zio-mavencentral` is a published Scala 3 / ZIO 2 library (`com.jamesward:zio-mavencentral_3`) for
working with Maven Central: the typed read API (`MavenCentral`), jar/zip downloads, the on-disk
`JarCache`, `GavCacheMiddleware` for zio-http, GAV `PathCodec`s, and the Sonatype Central Portal
publishing client (`MavenCentral.Deploy.Sonatype`). See `README.md` for the feature overview.
It is a library: releases go through sbt-ci-release with `versionScheme := Some("semver-spec")`.

Follow the `zen-of-projects` Skill (extract with `./sbt extractSkillsJars`); this file records only
project-specific facts and exceptions.

## Skills

- `zen-of-scala` — Scala 3 / ZIO idioms for all code and review.
- `zen-of-james` — design, domain modeling, effects, and testing principles.
- `zen-of-projects` — project/build conventions.

## Exceptions to zen-of-projects

- `dev.zio:zio-direct` has no stable release; keep it on its newest release (RC) as long as tests pass.

## Build, Test & Dev Workflow

- Use the MCP server named `sbt-mcp-zio-mavencentral` (`http://127.0.0.1:5121/`) for sbt interactions: `sbt-task` (separate commands with `;`),
  `list-tasks`, `check` (`"scope":"module"` after API changes), and `glob-search` / `inspect` /
  `symbol-location` plus the proxied javadocs.dev tools for symbol lookups. If it is unavailable,
  say so and fall back to the `./sbt` launcher.
  - Kiro: HTTP entry in `.kiro/settings/mcp.json`; start sbt first.
  - Claude Code: `.mcp.json` runs `.claude/sbt-mcp-stdio.sh` (approved in `.claude/settings.json`),
    a stdio bridge that starts a foreground sbt in cloud sessions (`CLAUDE_CODE_REMOTE=true`) and
    connects to an already-running sbt locally. Its tools are deferred: load them with ToolSearch
    (search `sbt-mcp-zio-mavencentral`). Diagnostics: `/tmp/sbt-mcp-stdio.log`,
    `/tmp/sbt-mcp-server.log`.
- Commands (CLI fallback form):
  - `./sbt "Test / compile"` — compile main + tests.
  - `./sbt testFull` — full test suite (what CI runs).
  - `./sbt "testOnly *MavenCentralSpec -- -t downloadAndExtractZip"` — a single test.
- `--sun-misc-unsafe-memory-access=allow` is added to forked test JVMs only when running on JDK 24+.
- Most tests hit the real (free) Maven Central over the network. The Sonatype `upload and verify`
  test publishes for real and only runs when `OSS_DEPLOY_USERNAME` and `OSS_DEPLOY_PASSWORD` are
  set — do not set them unless explicitly asked.

## Releasing

- Run the `Tag Release` workflow (or push a `v<major>.<minor>.<patch>` tag, see `DEV.md`);
  `release.yml` runs `testFull` then `ci-release`.
