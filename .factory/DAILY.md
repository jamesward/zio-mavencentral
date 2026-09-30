# Daily Routine — zio-mavencentral

Keep this library current and aligned with `AGENTS.md` and the `zen-of-projects` Skill, in a
single rolling PR.

If there are other open PRs for this work, update that PR instead of creating a new one.

1. Find the existing open PR for this routine first (title prefix `Daily maintenance:`):
   `gh pr list --state open --search 'in:title "Daily maintenance:"'`. If one exists, check out
   its branch and work there; only create a new branch/PR when none exists.
2. Preserve existing work: build on the PR branch and any uncommitted changes; never discard,
   revert, or force-push over someone else's commits.
3. Update dependencies: resolve the latest **stable** versions (no RC/M/alpha/beta/SNAPSHOT) of
   sbt (`project/build.properties`), Scala, every plugin in `project/plugins.sbt`, and every
   library in `build.sbt` (including the `com.jamesward:skills` `% Skills` dependency), and pin
   them exactly. Keep `zio-direct` on its latest release until a stable one exists. Review and
   fold in open Dependabot PRs where they apply.
4. Run `./sbt extractSkillsJars` and read the extracted `.kiro/skills/**/SKILL.md` files.
5. Align the project with `AGENTS.md` and the `zen-of-projects` Skill (flat `build.sbt`, sbt-mcp
   on port 5121 loopback-only, compiler flags, Java 21 in CI, launchers, `.gitignore`), and update
   `AGENTS.md` when the code or workflow has drifted from it.
6. Run CI locally with Java 21 and fix any failures:
   ```bash
   ./sbt shutdown
   ./sbt extractSkillsJars
   ./sbt "Test / compile; testFull"
   ./sbt shutdown
   ```
   Do not set `OSS_DEPLOY_USERNAME` / `OSS_DEPLOY_PASSWORD` (that test publishes to Sonatype).
7. Commit to the single PR branch and push; update the PR description so it summarizes the
   cumulative changes. If nothing changed, take no action.
