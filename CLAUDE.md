# CLAUDE.md

Project-specific context for Claude Code (or any AI assistant) working on this repository. Read this
before making structural changes — several things here look unusual on first glance but are deliberate.

## What this project is

Kotlin coroutine wrappers around spring-kafka's `CompletableFuture`/`ListenableFuture`-based Java APIs.
One module per supported spring-kafka version: `spring-kafka-kotlin-2.8` through `spring-kafka-kotlin-4.1`.
These 8 modules are the only real, published library artifacts. Everything else in the repo exists to
support them.

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for the design rationale behind specific wrapper decisions
(why `KafkaCoroutineOperations` exists instead of implementing `KafkaOperations` directly, why blocking
calls need a configurable dispatcher, why the repo is split one module per spring-kafka version, etc.) —
check it before second-guessing an existing design choice in the wrapper classes.

## Module architecture

### Shared source, not shared jars

`hook-aspect-native`, `hook-aspect-compat`, `micrometer-tags-provider-cache`,
`send-and-receive-result-unbounded`, `send-and-receive-result-bounded` are **not** dependencies of the
`spring-kafka-kotlin-*` modules. They're wired in via `sourceSets.main.kotlin.srcDir(...)` in each
consumer's `build.gradle.kts` — the `.kt` files get compiled directly into each consuming module, against
that module's own spring-kafka version and classpath. No shared jar, no runtime dependency on the shared
module. Each shared module also compiles standalone (for its own `compileKotlin`/tests), which is why some
of them have a `compileOnly(project(":spring-kafka-kotlin-X.Y"))` dependency purely so a receiver type
(like `ReplyingKafkaCoroutineOperations`) resolves at compile time — not a real runtime dependency, and not
a cycle (srcDir sharing doesn't create a reverse project dependency).

Only one `spring-kafka-kotlin-*` module can ever be on a classpath at a time: `hook-aspect-native` and
`hook-aspect-compat` deliberately compile classes under the *same* package/class names (for public API
parity across versions), so combining two consumer modules on one classpath would collide.

### Naming conventions

- `-native` / `-compat`: an API-shape split driven by a spring-kafka feature boundary. `hook-aspect-native`
  (spring-kafka 3.2+, suspend `@KafkaListener` support) vs `hook-aspect-compat` (2.8–3.1, backport
  approach). Chosen over version-range names after explicit research into Kotlin-stdlib/Guava precedent —
  see git history if the naming ever comes up again.
- `-bounded` / `-unbounded`: a generic-bounds split driven by JSpecify. spring-kafka 4.0 marks
  `org.springframework.kafka.core`/`.support`/`.requestreply` `@NullMarked` (JSpecify), which forces Kotlin
  generics like `SendAndReceiveResult<K, V, R>` to become `<K : Any, V : Any, R : Any>` from 4.0 onward.
  `send-and-receive-result-bounded` (4.0+) vs `send-and-receive-result-unbounded` (2.8–3.3).
- Every shared module's README documents exactly which consumers use it and why — check there before
  assuming a module applies more broadly than it does (e.g. `micrometer-tags-provider-cache` excludes 2.8;
  `hook-aspect-*` excludes different version ranges than the `send-and-receive-result-*` split).

### What NOT to extract into a shared module

Pure delegate-forwarding methods should stay duplicated per module — that's intentional, it keeps each
module's public API legible on its own without cross-referencing a shared file. Only extract genuinely
identical, non-trivial logic (e.g. `SendAndReceiveResult` + its reified extension functions were extracted
specifically because they're pure Kotlin convenience logic, not forwarders).

## Nullability and API correctness

- Always verify a delegate's real nullability from its actual Java source/bytecode (or the
  `@NonNullApi`/`@NullMarked`/`@Nullable` package-info annotations), never assume from version number or
  trust an existing code comment without checking. A stale `@Suppress` comment claiming a package
  "isn't under `@NonNullApi`" was found to be simply false during an audit — the annotation was there, the
  suppress was hiding a real bug (a genuinely nullable return incorrectly typed as non-null).
- Expose `kotlin.time.Duration`, not `java.time.Duration`, in every public API.
- Use native Kotlin lambdas/functions, not `java.util.Function`/other SAM types, in public APIs.
- `protected` on a Kotlin class that isn't `open` is dead code (final classes can't be subclassed, and
  Kotlin's `protected` is stricter than Java's — no same-package access either). If you see `protected` on
  a non-open class, it should almost certainly be `public`.

## Build system

- **Gradle 9.6.0**, Kotlin **2.2.21**. The Gradle daemon needs JVM 17+ to run at all (Gradle 9 requirement)
  — this is independent of what bytecode version individual modules *target* (2.8/2.9 target JVM 8 via
  `jvmTarget`/`sourceCompatibility`, and this has been verified to compile and run correctly under a
  JDK 17 daemon; no need for multiple JDKs installed).
- **Version catalog** (`gradle/libs.versions.toml`): one alias per spring-kafka/spring-aop version axis
  (never shared across modules — each module pins its own), but a single shared alias where multiple
  modules genuinely bundle the identical version (e.g. `aspectjweaver`, `kotlinx-coroutines`,
  `spring-aop-v32plus` covering 3.2/3.3/4.0/4.1).
- **`build-logic/`** is a separate included build (`pluginManagement { includeBuild("build-logic") }` in
  root `settings.gradle.kts`) holding convention plugins as precompiled script plugins. Currently just
  `spring-kafka-kotlin.published-module.gradle.kts` (Sonatype Central Portal publishing, see below).
  Being a separate build, it doesn't auto-detect the root's `gradle/libs.versions.toml` — its own
  `settings.gradle.kts` explicitly wires it in via `dependencyResolutionManagement { versionCatalogs { create("libs") { from(files("../gradle/libs.versions.toml")) } } }`,
  so `build-logic/build.gradle.kts` pins its own dependencies (`vanniktech-maven-publish`,
  `kotlin-gradle-plugin`) through the same catalog aliases rather than hardcoded version strings.
  `kotlin-gradle-plugin` deliberately reuses `version.ref = "kotlin"` so it can never drift out of sync
  with the rest of the project's Kotlin version.
  - **Important Gradle gotcha already hit once**: applying the same plugin independently in multiple
    sibling subprojects (rather than pre-applying with `apply false` at root first) gives each subproject
    its own classloader scope for that plugin. If the plugin registers a shared `BuildService` (like
    `com.vanniktech.maven.publish` does, to batch all modules' uploads into one Central Portal deployment),
    this causes a `Cannot set the value of task ... property 'buildService'` failure — it only shows up
    when Gradle configures multiple sibling projects in one invocation (e.g. IntelliJ sync), not when
    running against one module at a time. Fix: `id("spring-kafka-kotlin.published-module") apply false` in
    root's `plugins {}` block, same pattern already used for `kotlin.jvm`/`ktlint`. If you add a new
    convention plugin that multiple modules apply, pre-apply it at root the same way.
- **Publishing is opt-in, not opt-out**: only the 8 `spring-kafka-kotlin-*` modules apply
  `id("spring-kafka-kotlin.published-module")`. The shared-source modules and `benchmarks`/
  `benchmarks-compat` simply never apply it — no publication is ever registered for them, so there's
  nothing to disable. Do not add a `tasks.matching { it.name.startsWith("publish") }`-style guard to
  "prevent" publishing on a module; if a module shouldn't publish, don't apply the publishing plugin to it.
- **Versioning**: `com.palantir.git-version`, applied only at root, computes `project.version` from git
  tags (`v`-prefix stripped) via a custom 3-case scheme:
  - exactly on a tag, clean tree → the tag itself (e.g. `1.0.0`)
  - exactly on a tag, dirty tree → `1.0.0-SNAPSHOT`
  - commits since the last tag (clean or dirty) → `1.0.0-<shortHash>-SNAPSHOT`
  - Falls back to `0.0.0-SNAPSHOT` if the repo has no commits/tags yet (expected pre-first-release).
  - The plugin only ever exposes `versionDetails()` as an untyped Groovy `Closure` via extra properties —
    confirmed directly from its source, there's no typed Kotlin API to use instead. This is wrapped in a
    small `Project.versionDetails()` extension function in root `build.gradle.kts` so the cast is isolated
    in one place.
- **Publishing target**: Sonatype Central Portal via `com.vanniktech.maven.publish` (OSSRH is fully retired
  as of 2025-06-30 — don't reach for `gradle-nexus/publish-plugin`'s legacy OSSRH flow). `publishToMavenCentral`
  is used, not `publishAndReleaseToMavenCentral` — releases are staged for manual review, not automatic,
  since Central releases are effectively irreversible (immutable coordinates).
- **`gradle.properties`**: `org.gradle.jvmargs=-Xmx2g` and `kotlin.daemon.jvmargs=-Xmx2g` are set because
  Gradle 9's parallel execution across 15+ modules can OOM the default Kotlin compile daemon heap (512m).
  If you hit `OutOfMemoryError`/`GC overhead limit exceeded` during a build, check for stale Gradle daemons
  first (`./gradlew --stop`, then check `ps aux | grep -i gradledaemon` for orphaned processes from a
  previous Gradle version) before assuming it's a real regression.
- **Binary compatibility validation**: `org.jetbrains.kotlinx.binary-compatibility-validator`, applied
  once at root (this plugin's own convention — it configures every subproject internally rather than
  needing per-module opt-in, unlike `com.vanniktech.maven.publish`). `apiValidation { ignoredProjects.addAll(...) }`
  excludes the same 7 non-published modules (shared-source modules + `benchmarks`/`benchmarks-compat`) —
  only the 8 real `spring-kafka-kotlin-*` modules get an `api/<module-name>.api` dump and `apiCheck`
  (wired into `check`/`build`). After changing any public API, run `./gradlew apiDump` to regenerate the
  dump files and commit them alongside the change — `apiCheck` will otherwise fail the build on any
  intentional public-API change, not just accidental ones.
- **Explicit API mode**: `kotlin { explicitApi() }` (strict) in the `published-module` convention plugin —
  every public declaration across the 8 modules has an explicit visibility modifier and return type,
  matching the real delegate's type per declaration (verified against the actual spring-kafka sources
  rather than inferred/guessed). A missing `internal`/`private` or an inferred return type wider than
  intended now fails the build instead of silently shipping.
  - Applying `org.jetbrains.kotlin.jvm` a *second* time inside the convention plugin (every consuming
    module already applies it before applying the convention plugin) is required so Gradle generates the
    `kotlin { }` type-safe accessor for that precompiled script — this is a different situation from the
    `com.vanniktech.maven.publish` classloader gotcha above: re-applying the same plugin twice *within one
    project* is a harmless no-op (verified by compiling sibling modules together, same test used to catch
    the BuildService issue), it's only cross-*project* re-application of a plugin with a shared
    `BuildService` that breaks.

## Dependabot (`.github/dependabot.yml`)

Three `updates:` entries: a `gradle-wrapper` entry at root (`gradle/wrapper/gradle-wrapper.properties` —
a genuinely distinct `package-ecosystem` from `gradle`, needs its own explicit entry or Dependabot never
checks the wrapper version at all; no ignore rules, always safe to bump), a `gradle` entry at root (covers
the version catalog), plus one `gradle` entry for `build-logic` (its own separate Gradle project,
directory-scoped for Dependabot even though it now shares the root's version catalog file — see above).
Ignore rules on the two `gradle` entries classify every dependency as:
- **critical** (spring-kafka, spring-aop, aspectjweaver, kotlinx-coroutines-*, the
  `org.jetbrains.kotlin.jvm` compiler plugin) — patch updates only, minor and major both ignored, since
  these directly affect behavior the coroutine wrappers depend on matching exactly.
- **patch-only, but not because they're critical themselves**: `org.jetbrains.kotlin.plugin.spring` and
  `org.jetbrains.kotlin.plugin.allopen`. Verified neither affects any *shipped* module's runtime behavior:
  `plugin.spring` is only applied in `spring-kafka-kotlin-3.2`/`3.3` and only used by their own *test*
  source (`@Configuration`/`@Component` classes proving Spring integration — grepped `main` source in both
  modules, zero Spring stereotype annotations there); `plugin.allopen` is only applied in
  `benchmarks`/`benchmarks-compat`, never in a real `spring-kafka-kotlin-*` module. They're patch-only
  purely because they share `version.ref = "kotlin"` in the catalog with `org.jetbrains.kotlin.jvm` — all
  three are the same Kotlin release under different plugin ids, so they must move in lockstep with it
  rather than potentially drifting to a different Kotlin minor/major version via independent Dependabot PRs.
- **non-critical** (junit-bom, ktlint, kotlinx-benchmark/kotlinx-benchmark-runtime, palantir-git-version,
  vanniktech-maven-publish, binary-compatibility-validator) — minor updates allowed too, only major
  ignored. Test/benchmark/build/versioning/publishing/API-validation tooling, not something the coroutine
  wrappers' *shipped* runtime behavior depends on, and not tied to a shared version with anything critical.
- `build-logic`'s own `org.jetbrains.kotlin:kotlin-gradle-plugin` dependency (pinned only so the
  precompiled convention script can resolve the `kotlin { }` extension type, see above) is patch-only for
  the same reason as `plugin.spring`/`plugin.allopen` above — it shares `version.ref = "kotlin"` in the
  same catalog, so it moves in lockstep with the rest of the project's Kotlin version automatically
  rather than needing manual cross-directory syncing.
  - **Must be ignored in both Dependabot `updates:` entries, not just `/build-logic`'s own.**
    `gradle/libs.versions.toml` physically lives under the root directory, so Dependabot's root
    (`directory: "/"`) scan sees this catalog entry too, even though only `build-logic/build.gradle.kts`
    actually references it. Ignoring it only in the `/build-logic` entry left the root scan free to bump
    the shared `"kotlin"` version key past patch to satisfy this one ungated dependency, dragging
    `org.jetbrains.kotlin.jvm`/`plugin.spring`/`plugin.allopen` along with it (surfaced as a
    `Bump kotlin from 2.2.21 to 2.4.10` PR) — this was a real incident, not a hypothetical. Any future
    catalog entry that shares a `version.ref` with a critical/patch-only dependency needs its own ignore
    rule in *every* Dependabot directory scan that can see the catalog file, not just the one whose build
    script happens to consume it.

If you add a new dependency, decide which bucket it belongs in and add the matching `ignore` rule —
matching is by `dependency-name` (the resolved `group:artifact`), so one rule covers every catalog alias
pinning that artifact.

## CI (`.github/workflows/`)

- `ci.yml`: runs on every PR and on every push to `main` — `ktlintCheck` and `test` as separate steps
  (a lint failure should read distinctly from a test failure), plus `dorny/test-reporter` for a per-test
  breakdown and `gradle/actions/setup-gradle`'s job summary/PR comment on failure. The `push: branches:
  ["main"]` trigger exists specifically so the README's CI badge (which points at `?branch=main`) has a
  real run to report on — PR-only runs report against the PR's head branch, never `main` itself.
- `release.yml`: runs on pushing a `v*` tag — same lint/test gate, then `publishToMavenCentral`. Uses
  `fetch-depth: 0` on checkout (`com.palantir.git-version` needs full history/tags to `git describe`
  correctly — the default shallow checkout breaks this). Reads 5 secrets
  (`MAVEN_CENTRAL_USERNAME`/`PASSWORD`, `SIGNING_IN_MEMORY_KEY`/`_PASSWORD`/`_ID`) mapped to
  `ORG_GRADLE_PROJECT_`-prefixed env vars.
- `codeql.yml`: GitHub's native security scanning, on every push/PR to `main` plus a weekly schedule.
  Language is `java-kotlin` (CodeQL treats Java and Kotlin as one combined language pack, not separate
  identifiers). Builds manually (`./gradlew build -x test -x ktlintCheck -x apiCheck`) rather than using
  the `autobuild` action, matching how the other workflows already invoke Gradle directly.
- `dependency-review.yml`: `actions/dependency-review-action`, runs on every PR — fails if a dependency
  change introduces a known-vulnerable or license-incompatible package. Only works on `pull_request`
  events (it diffs base vs head dependency manifests), can't run on push/schedule.

## Verifying changes

After any non-trivial change, the standard verification sequence used throughout this project's
development is:

```bash
./gradlew ktlintFormat -q
./gradlew clean build -q
find . -path "*/build/test-results/test/TEST-*.xml" -exec grep -H 'testsuite name' {} \; | \
  grep -o 'tests="[0-9]*" skipped="[0-9]*" failures="[0-9]*" errors="[0-9]*"' | \
  awk -F'"' '{s+=$2; f+=$4; e+=$6} END {print "total tests:", s, "failures:", f, "errors:", e}'
```

At the time of writing this is 431 tests, 0 failures, 0 errors — that number will grow as more tests are
added; the point is to confirm it doesn't *shrink* or gain failures after a change. A pure refactor
(extracting shared source, renaming modules, changing build wiring) should never change the test count.

If a change touches publishing config, verify structurally with a **temporary** local repository
(`maven { url = uri("$rootDir/build/local-repo") }`) rather than actually publishing anywhere — check which
modules produced artifacts, inspect POM dependency lists for anything unexpected (e.g. a leaked
`project(...)` reference), then delete `build/local-repo` and revert the temporary repository config
before finishing. Never leave real publish credentials or a real repository target added "just to test."

## Environment quirks (this dev machine, may not apply everywhere)

- Unquoted multi-colon Gradle task paths (`:module:task`) inside shell loops have occasionally been
  mangled by this environment's shell/tooling. Quote each full task path as its own argument to a single
  `./gradlew` invocation instead of interpolating into a loop.
- `grep` is aliased to `ugrep` here, which can behave oddly with multi-file glob output. Prefer
  `grep -rl ... > file.txt` + `while read` loops, or `find ... -exec ... \;`, over piping through `xargs`.
