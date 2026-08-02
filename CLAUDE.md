# CLAUDE.md

Project-specific context for Claude Code (or any AI assistant) working on this repository. Read this
before making structural changes — several things here look unusual on first glance but are deliberate.

## What this project is

Kotlin coroutine wrappers around spring-kafka's `CompletableFuture`/`ListenableFuture`-based Java APIs.
One module per supported spring-kafka version: `spring-kafka-kotlin-2.8` through `spring-kafka-kotlin-4.1`.
Each of these also has a matching `spring-kafka-kotlin-X.Y-boot` module providing Spring Boot
auto-configuration. These 16 modules are the only real, published library artifacts. Everything else in
the repo exists to support them.

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for the design rationale behind specific wrapper decisions
(why `KafkaCoroutineOperations` exists instead of implementing `KafkaOperations` directly, why blocking
calls need a configurable dispatcher, why the repo is split one module per spring-kafka version, etc.) —
check it before second-guessing an existing design choice in the wrapper classes.

## Module architecture

### Shared source, not shared jars

`hook-aspect-native`, `hook-aspect-compat`, `micrometer-tags-provider-cache`,
`send-and-receive-result-unbounded`, `send-and-receive-result-bounded`, `boot-autoconfiguration-unbounded`,
`boot-autoconfiguration-bounded`, `boot-dispatcher-test-legacy`, `boot-dispatcher-test-unbounded`,
`boot-dispatcher-test-bounded` are **not** dependencies of the `spring-kafka-kotlin-*`/`-boot` modules.
They're wired in via `sourceSets.main.kotlin.srcDir(...)` (or `sourceSets.test.kotlin.srcDir(...)` for the
`boot-dispatcher-test-*` modules) in each consumer's `build.gradle.kts` — the `.kt` files get compiled
directly into each consuming module, against that module's own spring-kafka version and classpath. No
shared jar, no runtime dependency on the shared module. Each shared module also compiles standalone (for
its own `compileKotlin`/tests), which is why some of them have a `compileOnly`/`testImplementation
project(":spring-kafka-kotlin-X.Y")` dependency purely so a receiver type (like
`ReplyingKafkaCoroutineOperations`, or `KafkaCoroutineTemplate`/`KafkaCoroutineTemplateAutoConfiguration`
for the `boot-*` shared modules) resolves at compile time — not a real runtime dependency, and not a cycle
(srcDir sharing doesn't create a reverse project dependency).

The `boot-dispatcher-test-*` modules are the first in this repo to share `src/test` rather than `src/main`
— mechanically identical (`sourceSets.test.kotlin.srcDir(...)` instead of `.main.`), just a source set
Gradle/this repo hadn't shared before. Each has no `src/main` of its own at all.

Only one `spring-kafka-kotlin-*` module can ever be on a classpath at a time: `hook-aspect-native` and
`hook-aspect-compat` deliberately compile classes under the *same* package/class names (for public API
parity across versions), so combining two consumer modules on one classpath would collide.

### The `-boot` modules

Each `spring-kafka-kotlin-X.Y` module has a sibling `spring-kafka-kotlin-X.Y-boot` module (`api(project(":spring-kafka-kotlin-X.Y"))`)
providing a `KafkaCoroutineTemplateAutoConfiguration` — a `@Configuration(proxyBeanMethods = false)` class
exposing a `KafkaCoroutineTemplate` bean built from whatever `KafkaTemplate` bean the application's own
Spring Boot `KafkaAutoConfiguration` already provides (`@ConditionalOnBean(KafkaTemplate::class)`,
backs off via `@ConditionalOnMissingBean(KafkaCoroutineTemplate::class)`). Mirrors the pattern in the
sibling project `spring-virtual-reactor` (`CoroutineTransactionTemplate`/
`CoroutineTransactionTemplateAutoConfiguration`).

- `spring-boot-autoconfigure` is a genuine new dependency here, not something pulled in transitively —
  confirmed directly from spring-kafka's own POM, which has zero dependency on `spring-boot`/
  `spring-boot-autoconfigure` in either direction (it's `spring-boot-autoconfigure` that optionally depends
  on `spring-kafka`, not the reverse). Each `-boot` module only needs it `compileOnly`/`testImplementation`
  (the "provided" pattern already used for spring-kafka itself in the non-boot modules) — a real Spring
  Boot application supplies it via its own starter at runtime.
- Every `-boot` module applies `kotlin("plugin.spring")` (`alias(libs.plugins.kotlin.plugin.spring)`),
  even the ones whose base module doesn't (only `3.2`/`3.3` apply it, and only to their own test source —
  see Dependabot section below). Reason: Kotlin classes are `final` by default, but Spring's default
  (non-`proxyBeanMethods = false`) `@Configuration` mode requires CGLIB subclassing of non-final classes —
  this plugin auto-opens `@Configuration`/`@Component`-annotated classes so nested test `@Configuration`
  classes (which don't set `proxyBeanMethods = false`, unlike the shipped auto-configuration itself) don't
  need `open` added by hand. Discovered via a real `BeanDefinitionParsingException: ... may not be final`
  test failure the first time a `-boot` module's test suite was written. The 3 `boot-dispatcher-test-*`
  shared modules need it too, for the exact same reason applied to their own standalone `test` runs — easy
  to miss since it doesn't show up until that module's *own* tests run, not just the consumers'.
- `KafkaCoroutineTemplateAutoConfiguration`/`ReplyingKafkaCoroutineTemplateAutoConfiguration`/
  `RoutingKafkaCoroutineTemplateAutoConfiguration`/`AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration`
  themselves and their dispatcher-wiring tests don't live directly in any `-boot` module's own source — see
  `boot-autoconfiguration-unbounded`/`-bounded` and `boot-dispatcher-test-legacy`/`-unbounded`/`-bounded`
  above. Only `AutoConfigurationImportsTest` and the auto-discovery resource file stay per-module: their
  natural sharing boundary (2.8 solo, 2.9 solo, 3.0–4.1 shared) doesn't line up with either the
  bounded/unbounded split or any other grouping used elsewhere in this repo, so extracting them would add
  a shared module whose membership doesn't match the naming pattern of the others, for a one-line resource
  file and a ~20-line test.
- `ReplyingKafkaCoroutineTemplateAutoConfiguration` mirrors `KafkaCoroutineTemplateAutoConfiguration` in
  every respect (same unbounded/bounded generic split — `<K, V, R>` vs `<K : Any, V : Any, R : Any>` — same
  `blockingIODispatcher` qualifier reuse, same `@ConditionalOnMissingBean` "customize-or-default" reasoning)
  with one difference: it's gated on `@ConditionalOnBean(ReplyingKafkaTemplate::class)`, not `KafkaTemplate`.
  Unlike `KafkaTemplate`, Spring Boot's own `KafkaAutoConfiguration` never provides a `ReplyingKafkaTemplate`
  bean automatically — request/reply needs an application-specific reply container/topic — so this
  auto-configuration only ever activates once the application has registered its own `ReplyingKafkaTemplate`
  bean (which it needs to do anyway, for that bean's own `SmartLifecycle` callbacks to fire; see
  `ReplyingKafkaCoroutineTemplate`'s own doc comment). Its own wiring test registers that bean with
  `isAutoStartup = false` so `context.refresh()` never attempts a real broker connection.
- `RoutingKafkaCoroutineTemplateAutoConfiguration` also mirrors the same shape (gated on
  `@ConditionalOnBean(RoutingKafkaTemplate::class)`, same reasoning as `ReplyingKafkaTemplate` for why Boot
  never provides one automatically — it needs an application-specific map of topic-pattern to
  `ProducerFactory`), but with one structural difference from the other two: `RoutingKafkaTemplate` is
  fixed to `KafkaTemplate<Object, Object>` (not generic), so `RoutingKafkaCoroutineTemplateAutoConfiguration`'s
  `@Bean` method has no type parameters at all, and its source file is **byte-identical** between the
  unbounded and bounded groups — it's placed in both `boot-autoconfiguration-unbounded` and
  `boot-autoconfiguration-bounded` unchanged, rather than needing two different variants like the other two
  auto-configurations. `RoutingKafkaTemplate` also has no `SmartLifecycle` of its own (only
  `ApplicationContextAware`/`BeanNameAware`/`SmartInitializingSingleton`), so unlike the `Replying` wiring
  test, its own test doesn't need `isAutoStartup = false` to avoid a real connection attempt.
- `AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration` mirrors `ReplyingKafkaCoroutineTemplateAutoConfiguration`
  almost exactly (`AggregatingReplyingKafkaTemplate` extends `ReplyingKafkaTemplate`, same unbounded/bounded
  split, same `SmartLifecycle`/`isAutoStartup = false` test handling, gated on
  `@ConditionalOnBean(AggregatingReplyingKafkaTemplate::class)` for the same "Boot never provides one
  automatically" reason). The one extra wrinkle carried into its own wiring test's fixture:
  `AggregatingReplyingKafkaTemplate` asserts in its own constructor that the reply container uses
  `ContainerProperties.AckMode.MANUAL` (it manages its own offset commits as replies are aggregated), so
  the test's unstarted container must set that explicitly or construction itself throws, before
  `autoStartup` is even relevant.
- **`KafkaCoroutineTemplateAutoConfiguration` needs `@AutoConfigureAfter(KafkaAutoConfiguration::class)`** —
  a real bug found (and fixed) by testing with Spring Boot's own recommended auto-configuration test
  harness (`ApplicationContextRunner` + `AutoConfigurations.of(...)`), not just this repo's own
  `context.register(...)`-based tests. `AutoConfigurationSorter` (`spring-boot-autoconfigure`'s own source)
  falls back to **plain alphabetical class-name sorting** when no `@AutoConfigureAfter`/`@AutoConfigureBefore`/
  `@AutoConfigureOrder` is declared — `org.mdedetrich...` sorts before `org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration`
  (or `org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration` on Boot 4, see below), so without
  this annotation, `KafkaCoroutineTemplateAutoConfiguration` was processed *before* Boot's own
  `KafkaAutoConfiguration` in a real app — meaning `@ConditionalOnBean(KafkaTemplate::class)` saw no
  `KafkaTemplate` bean yet and silently never fired. Confirmed both ways with a real
  `ApplicationContextRunner`: without the annotation, `KafkaCoroutineTemplate present: []`; with it,
  `KafkaCoroutineTemplate present: [kafkaCoroutineTemplate]`. This repo's own
  `KafkaCoroutineTemplateAutoConfigurationTest`-style tests never caught this, since manually calling
  `context.register(...)` bypasses `AutoConfigurationImportSelector`/`AutoConfigurationSorter` entirely —
  they only prove the conditionals are internally consistent, not that they fire in the right order in a
  real app. `ReplyingKafkaCoroutineTemplateAutoConfiguration`/`RoutingKafkaCoroutineTemplateAutoConfiguration`/
  `AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration` do **not** need the same fix — confirmed via
  the same real-harness approach — because their target beans (`ReplyingKafkaTemplate`/`RoutingKafkaTemplate`/
  `AggregatingReplyingKafkaTemplate`) are always user-defined, never created by another auto-configuration,
  and Spring Boot guarantees `@ConditionalOnBean`/`@ConditionalOnMissingBean` on auto-configuration classes
  "are guaranteed to load after any user-defined bean definitions have been added" regardless of
  alphabetical sort position — the ordering hazard only exists when the condition targets a bean created by
  *another auto-configuration class*.
- **Spring Boot 4.0 moved `KafkaAutoConfiguration` out of `spring-boot-autoconfigure` entirely**, into a new
  dedicated `org.springframework.boot:spring-boot-kafka` artifact, under a new package
  `org.springframework.boot.kafka.autoconfigure` (not `org.springframework.boot.autoconfigure.kafka`, its
  2.8–3.3 location) — part of Boot 4's broader modularization of `spring-boot-autoconfigure` into
  per-technology jars. Confirmed by downloading and inspecting the real `spring-boot-autoconfigure-4.0.7`
  jar directly (no `kafka` package inside at all) and the real `spring-boot-kafka-4.0.7`/`-4.1.0` jars
  (`KafkaAutoConfiguration.class` present in both). `boot-autoconfiguration-bounded` and
  `spring-kafka-kotlin-4.0-boot`/`4.1-boot` depend on the new `spring-boot-kafka-v40`/`v41` catalog alias
  (`compileOnly`/`testImplementation`, same "provided" pattern) specifically for this import;
  `boot-autoconfiguration-unbounded` (2.8–3.3) keeps importing from `spring-boot-autoconfigure` as before,
  since the split only happened at 4.0.
- **`AutoConfigurationOrderingTest`** (in each `boot-dispatcher-test-*` module) is permanent regression
  coverage for the `@AutoConfigureAfter` bug above, using Spring Boot's own recommended way to test an
  auto-configuration (`ApplicationContextRunner` + `AutoConfigurations.of(...)`) rather than this repo's own
  `context.register(...)`-based tests, which cannot catch ordering bugs since they bypass
  `AutoConfigurationImportSelector`/`AutoConfigurationSorter` entirely. Needs `spring-boot-test` +
  `assertj-core` as new test-only dependencies (`spring-boot-test-vXY`/`assertj-core` catalog aliases) —
  `assertj-core` is required purely because `ApplicationContextRunner`'s own callback type
  (`AssertableApplicationContext`) extends assertj's `AssertProvider`, needed to resolve at compile time
  even though these tests never call an assertj assertion method themselves. Covers all four
  auto-configurations: confirms `KafkaCoroutineTemplateAutoConfiguration` wires when run after Boot's own
  `KafkaAutoConfiguration`, and confirms the other three wire with a user-defined delegate and no Boot
  auto-configuration involved at all (proving they don't need the same fix, not just assuming it). Also
  covers a second real bug all four shared: `@ConditionalOnBean` only checks a bean exists, so multiple
  delegate beans (e.g. two `KafkaTemplate`s) with none `@Primary` passed the condition but crashed context
  refresh on the `@Bean` method's unqualified injection (`UnsatisfiedDependencyException: ... expected
  single matching bean but found N`) — reproduced directly before fixing. Switched all four to
  `@ConditionalOnSingleCandidate`, which backs off cleanly in that case and still resolves correctly when
  one delegate is `@Primary`; both proven per auto-configuration.
- **`spring-boot-autoconfigure-processor`** is applied via `kapt` (a Java-only annotation processor, no
  Kotlin/KSP equivalent exists) to every `-boot` module, generating
  `META-INF/spring-autoconfigure-metadata.properties` — a pre-parsed summary of each auto-configuration's
  conditions (including the `@AutoConfigureAfter` from the bug fix above) that lets
  `AutoConfigurationSorter`/`AutoConfigurationImportFilter` skip full ASM classloading of every candidate
  purely to check conditions. Pure Boot-startup-performance optimization, not a correctness requirement —
  confirmed by `AutoConfigurationOrderingTest` passing identically with or without it. No manual wiring is
  needed for the generated file to reach the jar — just `id("org.jetbrains.kotlin.kapt")` +
  `kapt(libs.spring.boot.autoconfigure.processor.vXY)`, nothing else; kapt's own Gradle integration already
  registers its output directory as part of the main source set's `classesDirs`, which the `jar` task
  already packages by default. An earlier version of this repo manually re-added that directory as a
  `resources.srcDir` "just in case," which only caused a duplicate-entry error that then had to be papered
  over with `duplicatesStrategy = EXCLUDE` — a self-inflicted problem. Don't reintroduce either without
  first confirming the file is actually missing from a real built jar (`unzip -l`).
- Auto-discovery mechanism is **not uniform across versions** — verified against each Spring Boot version's
  actual source before assuming the modern mechanism applies:
  - `2.9`–`4.1` (Spring Boot 2.7+): `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
    (one FQCN per line), verified in tests via `org.springframework.boot.context.annotation.ImportCandidates`
    (`spring-boot` core artifact, test-only — see its own catalog alias doc for why it's not a main-source
    dependency). `ImportCandidates.getCandidates()` was only added in Boot 3.0 — on `2.9` (Boot 2.7.18) the
    test must use `ImportCandidates` as the `Iterable<String>` it already implements (`.contains(...)`
    directly on the loaded instance), not a `.candidates` property access, or it fails to compile.
  - `2.8` (Spring Boot 2.6, predates the imports-file mechanism entirely — added in Boot 2.7): legacy
    `META-INF/spring.factories` keyed under `org.springframework.boot.autoconfigure.EnableAutoConfiguration`,
    verified in tests via `org.springframework.core.io.support.SpringFactoriesLoader.loadFactoryNames(...)`
    instead of `ImportCandidates` (which doesn't exist in Boot 2.6 at all).
- Generic bounds on `KafkaCoroutineTemplateAutoConfiguration`'s `@Bean` method match the base module's own
  `KafkaCoroutineTemplate` signature: unbounded `<K, V>` for `2.8`–`3.3`, bounded `<K : Any, V : Any>` for
  `4.0`/`4.1` (spring-kafka 4.0+'s JSpecify `@NullMarked` packages force the bound — see the bounded/unbounded
  `send-and-receive-result-*` split above for the same root cause).
- `KafkaCoroutineTemplate`'s `blockingIODispatcher: CoroutineDispatcher?` constructor parameter is wired
  through as an `ObjectProvider<CoroutineDispatcher>` bean-method parameter, resolved via
  `getIfUnique { Dispatchers.IO }` (falls back to the class's own default rather than `null`, since `null`
  is itself a meaningful value on the class — "run on the caller's dispatcher" — not merely "unconfigured").
  The `ObjectProvider` parameter carries a dedicated `@BlockingIODispatcher` marker annotation (defined once
  in `boot-autoconfiguration-unbounded`/`-bounded`'s own `BlockingIODispatcher.kt`, meta-annotated
  `@Qualifier`) rather than matching any `CoroutineDispatcher` bean in the context — an unqualified lookup
  would let an application's own unrelated `CoroutineDispatcher` bean (for some other feature entirely) get
  silently picked up here. A dedicated marker annotation rather than a raw string-literal `@Qualifier` value
  was a deliberate correction: an earlier version used `@Qualifier("kafkaCoroutineTemplateBlockingIODispatcher")`
  with a public `const val`, which — while functionally fine (confirmed against real Spring Boot source,
  `TaskExecutorConfigurations`/`TaskExecutionAutoConfiguration.APPLICATION_TASK_EXECUTOR_BEAN_NAME`, that
  `ObjectProvider` + named/qualified lookup is the idiomatic Boot pattern for "an optional bean of a common,
  reusable type many things could plausibly provide for unrelated reasons") — turned out to deviate from
  both Spring's own qualifier-naming guidance (values should be short semantic words like `main`/`EMEA`/
  `persistent`, not bean-name-style compound strings) and Boot's own closer precedent for this exact problem
  shape: `@FlywayDataSource`/`@BatchDataSource`/`@QuartzDataSource`/`@LiquibaseDataSource`, all four a tiny
  marker annotation meta-annotated `@Qualifier`, no string value at all. `BlockingIODispatcher` matches that
  pattern instead, translated to Kotlin per Spring Framework's own Kotlin qualifier reference example
  (`@Target(AnnotationTarget...)`/`@Retention(AnnotationRetention.RUNTIME)` rather than the
  `java.lang.annotation` equivalents `@FlywayDataSource` itself uses, since it's a `.java` file and ours
  isn't) — needs three targets, each verified by actually compiling the corresponding shape rather than
  assumed from the docs: `AnnotationTarget.VALUE_PARAMETER` (the auto-configuration's own consuming
  parameter — also covers a consumer's constructor-`val`/setter-parameter injection of the same qualified
  bean), `AnnotationTarget.FUNCTION` (the `@Bean` methods in `boot-dispatcher-test-*` that supply the
  qualified dispatcher in tests), and `AnnotationTarget.FIELD` (a consumer reusing the same qualified bean
  via plain field injection elsewhere in their own app — omitting it fails to compile with "this annotation
  is not applicable to target 'member property with backing field'"). Matches Spring Framework's own Kotlin
  qualifier reference example exactly (`@Target(AnnotationTarget.FIELD, AnnotationTarget.VALUE_PARAMETER)`),
  which already includes `FIELD` for the same reason. Unlike Boot's own `Executor`/`TaskExecutor` case, this
  module deliberately does
  *not* back off entirely when *any* `CoroutineDispatcher` bean exists in the context (via a class-level
  `@ConditionalOnMissingBean(CoroutineDispatcher::class)`) — `CoroutineDispatcher` isn't a single canonical
  app-wide bean the way `TaskExecutor` is, so an unrelated dispatcher bean elsewhere in the app must not
  suppress this auto-configuration's own use of it.
- Confirmed (researched, not assumed) that Spring has no existing bean/qualifier/annotation for "the
  dispatcher blocking coroutine work runs on" that `@BlockingIODispatcher` could have reused instead of
  being invented here:
  - Reactor's own equivalent concept, `Schedulers.boundedElastic()`, isn't a bean at all — it's a static
    singleton called directly at each use site (`Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())`).
    Reactor sidesteps the DI/qualifier question entirely by never injecting it, which is why nothing
    upstream had a qualifier to copy.
  - Spring Framework itself has this exact question open and unresolved: see
    [spring-projects/spring-framework#33788](https://github.com/spring-projects/spring-framework/issues/33788)
    ("Explore leveraging Virtual Thread Coroutine dispatcher") — argues the framework's current default for
    suspending controllers (`Dispatchers.Unconfined`) is actively wrong for blocking work (cites a benchmark:
    1000 requests take 1001s on the shared executor vs 17s on `Dispatchers.IO` vs 5s on a virtual-thread
    dispatcher), proposes Spring pick a real default. Assigned to a core maintainer, no resolution as of
    writing. If Spring's own team hasn't settled what "the" blocking-work `CoroutineDispatcher` should even
    be, there is no shipped qualifier anywhere downstream to have reused.
  - The closest thing that does exist is Spring Boot's own `applicationTaskExecutor` bean (required by
    Spring MVC/WebFlux/GraphQL, `AsyncTaskExecutor` type, resolved by bean-name convention rather than
    `@Qualifier`) — convertible to a `CoroutineDispatcher` via `.asCoroutineDispatcher()`, but Boot doesn't
    do that conversion itself and nothing wires it to coroutine consumers automatically. A user wanting one
    shared pool across both `@Async` and this library's coroutine templates would still have to bridge it
    into `@BlockingIODispatcher` by hand; revisit this default if #33788 ever ships an official answer.
  - See [docs.spring.io — Task Execution and Scheduling](https://docs.spring.io/spring-boot/reference/features/task-execution-and-scheduling.html)
    and [spring-projects/spring-boot#18269](https://github.com/spring-projects/spring-boot/issues/18269)
    (`Schedulers.boundedElastic()` adoption discussion) for the supporting detail above.

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
  vanniktech-maven-publish, binary-compatibility-validator, `assertj-core`) — minor updates allowed too,
  only major ignored. Test/benchmark/build/versioning/publishing/API-validation tooling, not something the
  coroutine wrappers' *shipped* runtime behavior depends on, and not tied to a shared version with anything
  critical. `assertj-core` belongs here rather than the `spring-boot-*` patch-only bucket below — it's
  purely test-only (resolves `AssertableApplicationContext`'s `AssertProvider` supertype at compile time, no
  assertj assertion method is ever called) and isn't paired to any Spring Boot version, so a minor bump
  carries none of that bucket's drift risk.
- **patch-only**: every `spring-boot-*` dependency added for the `-boot` modules' Spring Boot
  auto-configuration support — `spring-boot`, `spring-boot-autoconfigure`, `spring-boot-kafka`,
  `spring-boot-test`, `spring-boot-autoconfigure-processor`. Kept patch-only rather than sorted into
  non-critical, since these move in lockstep with the Spring Boot version each `-boot` module targets
  (itself tracking a specific spring-kafka release) — a minor/major bump could silently drift that pairing
  out of the version this repo has actually verified.
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

At the time of writing this is 809 tests, 0 failures, 0 errors — that number will grow as more tests are
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
