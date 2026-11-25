# AGENTS.md — zio-mavencentral

`zio-mavencentral` is a published Scala 3 / ZIO 2 library (`com.jamesward:zio-mavencentral_3`) for
working with Maven Central: the typed read API (`MavenCentral`), jar/zip downloads, the on-disk
`JarCache`, `GavCacheMiddleware` for zio-http, GAV `PathCodec`s, and the Sonatype Central Portal
publishing client (`MavenCentral.Deploy.Sonatype`). See `README.md` for the feature overview.
It is a library: releases go through sbt-ci-release with `versionScheme := Some("semver-spec")`.

Follow the `zen-of-projects` Skill; this file records only project-specific facts and exceptions.

## Skills

Extract the Skills with `./sbta extractSkillsJars` (into `.kiro/skills/`) and read the relevant ones before working.

- For Scala 3 and ZIO code (writing or reviewing it), follow `zen-of-scala`.
- For design, domain modeling, effects and testing strategy, follow `zen-of-james`.
- For project and build conventions, follow `zen-of-projects`.

## Exceptions to zen-of-projects

- `dev.zio:zio-direct` has no stable release; keep it on its newest release (RC) as long as tests pass.

## Build, Test & Dev Workflow

- Use the MCP server named `sbta` for sbt interactions. It is the stdio server `./sbta --mcp` (configured in
  `.mcp.json` for Claude Code and `.kiro/settings/mcp.json` for Kiro), which starts sbt itself when it isn't
  running. In Claude Code its tools are deferred: load them with ToolSearch (search `sbta`). Prefer the
  structured tools (`compile`, `test` with `full`, `check` after edits, ...) and `sbt-task` for anything else.
  If it is unavailable, say so and fall back to the `./sbta` launcher. Logs: `project/target/sbta/mcp.log`
  and `project/target/sbta/boot.log`.
- Commands (CLI fallback form):
  - `./sbta "Test / compile"` — compile main + tests.
  - `./sbta testFull` — full test suite (what CI runs).
  - `./sbta "Test / compile; testFull"` — full validation (maintenance routine).
  - `./sbta "testOnly *MavenCentralSpec -- -t downloadAndExtractZip"` — a single test.
- `--sun-misc-unsafe-memory-access=allow` is added to forked test JVMs only when running on JDK 24+.
- Most tests hit the real (free) Maven Central over the network. The Sonatype `upload and verify`
  test publishes for real and only runs when `OSS_DEPLOY_USERNAME` and `OSS_DEPLOY_PASSWORD` are
  set — do not set them unless explicitly asked.
- In Claude Code cloud sessions, `MavenCentralSpec` "coordinates Maven Central rejects with 400 are
  not found, not errors" fails for the 1,200-character artifactId: the session's egress answers
  zio-http's HEAD for that path with 503 (curl gets repo1's 400), so `javadocUri` fails with
  `NotFoundOnFallback`. It passes in CI; there, treat the PR's CI as the authority for that test.

## Maintenance routine

`.factory/MAINTENANCE.md`, run on a schedule by the factory.

## Releasing

- Run the `Tag Release` workflow (or push a `v<major>.<minor>.<patch>` tag, see `DEV.md`);
  `release.yml` runs `testFull` then `ci-release`.
