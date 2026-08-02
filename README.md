# spring-kafka-kotlin

[![CI](https://github.com/mdedetrich/spring-kafka-kotlin/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/mdedetrich/spring-kafka-kotlin/actions/workflows/ci.yml?query=branch%3Amain)
[![CodeQL](https://github.com/mdedetrich/spring-kafka-kotlin/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/mdedetrich/spring-kafka-kotlin/actions/workflows/codeql.yml?query=branch%3Amain)
[![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-4.1?label=maven%20central)](https://central.sonatype.com/namespace/org.mdedetrich)
[![Kotlin](https://img.shields.io/badge/kotlin-7F52FF?logo=kotlin&logoColor=white)](https://kotlinlang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

Kotlin-first abstractions for using `spring-kafka` from Kotlin — coroutine-native
APIs in place of `CompletableFuture` (`KafkaTemplate`, `ReplyingKafkaTemplate`,
`RoutingKafkaTemplate`, `AggregatingReplyingKafkaTemplate`), a hook for running
suspend `@KafkaListener` methods in custom coroutine contexts, plus Kotlin
quality-of-life improvements.

See [USAGE.md](USAGE.md) for side-by-side `spring-kafka` vs `spring-kafka-kotlin` code
comparisons.

## Coroutine-native templates (`KafkaCoroutineTemplate` and friends)

### Coroutines instead of `CompletableFuture`

`send`, `sendAndReceive`, and friends are `suspend fun`s, not
`CompletableFuture`-returning methods — call them directly, no `.await()` or
`kotlinx-coroutines-jdk8` bridging needed.

`receive` (the one genuinely blocking, poll-based operation) is also a
`suspend fun`. It runs on a configurable dispatcher, defaulting to
`Dispatchers.IO`:

```kotlin
@Bean
fun kafkaCoroutineTemplate(kafkaTemplate: KafkaTemplate<String, String>): KafkaCoroutineTemplate<String, String> =
    KafkaCoroutineTemplate(kafkaTemplate) // blockingIODispatcher defaults to Dispatchers.IO
```

Pass a different `CoroutineDispatcher`, or `blockingIODispatcher = null` to call the
delegate directly on the caller's own dispatcher (e.g. when using a
virtual-thread-per-task dispatcher, where the extra hop isn't needed). See
[IMPLEMENTATION.md](IMPLEMENTATION.md) for why this is necessary.

The same suspend-function treatment, including the configurable
`blockingIODispatcher`, is available on `ReplyingKafkaCoroutineTemplate`,
`RoutingKafkaCoroutineTemplate`, and `AggregatingReplyingKafkaCoroutineTemplate` —
coroutine equivalents of `ReplyingKafkaTemplate`, `RoutingKafkaTemplate`, and
`AggregatingReplyingKafkaTemplate` respectively.

### Request/reply extras

Every `sendAndReceive` overload (on `ReplyingKafkaCoroutineTemplate` and
`AggregatingReplyingKafkaCoroutineTemplate`) has a `sendAndReceiveDeferred`
counterpart that returns immediately with the request-publish outcome and the
reply as independent `Deferred`s, instead of suspending for the full round
trip — await either on its own:

```kotlin
val result = replyingTemplate.sendAndReceiveDeferred(record)
result.sendResult.await() // confirms the request was published
val reply = result.reply.await() // the correlated reply, whenever it arrives
```

For the `ParameterizedTypeReference<P>`-typed overloads, `sendAndReceiveTyped`/
`sendAndReceiveDeferredTyped` extension functions use a reified type argument
instead of requiring `object : ParameterizedTypeReference<P>() {}` boilerplate:

```kotlin
val reply: Message<Order> = replyingTemplate.sendAndReceiveTyped(message)
```

## A `CoWebFilter`-style hook for `@KafkaListener`

Spring WebFlux's `CoWebFilter` lets you wrap a suspend handler call by calling `chain.filter(exchange)`
however you like — install something into the coroutine context first, run code after it completes, skip
it entirely, whatever. `KafkaListenerCoroutineHookAspect` (package `...kotlin.aop`) is the same
idea for suspend `@KafkaListener` methods: `KafkaListenerCoroutineHook` is an abstract class with one
abstract suspend method, meant to be subclassed and registered with `@Component`, that receives the
listener's resolved invocation details and a `processMessage` suspend function representing the real
listener body, and decides how to call it:

```kotlin
data class CorrelationId(val value: String) : AbstractCoroutineContextElement(CorrelationId) {
    companion object Key : CoroutineContext.Key<CorrelationId>
}

@Component
class CorrelationIdHook : KafkaListenerCoroutineHook() {
    override suspend fun hook(invocation: KafkaListenerInvocation, processMessage: suspend () -> Any?): Any? {
        val correlationId =
            when (invocation) {
                is KafkaListenerInvocation.SingleRecord ->
                    invocation.record.headers().lastHeader("correlationId")?.value()?.let(::String)
                is KafkaListenerInvocation.BatchRecords ->
                    invocation.records.firstOrNull()?.headers()?.lastHeader("correlationId")?.value()?.let(::String)
                is KafkaListenerInvocation.IndividualParameters -> invocation.headers["correlationId"] as? String
            }
        return if (correlationId != null) withContext(CorrelationId(correlationId)) { processMessage() } else processMessage()
    }
}

@Configuration
@EnableAspectJAutoProxy
class KafkaListenerHookConfig {
    @Bean
    fun kafkaListenerCoroutineHookAspect(
        hook: KafkaListenerCoroutineHook,
    ) = KafkaListenerCoroutineHookAspect(hook)
}

@Component
class OrderListener {
    @KafkaListener(topics = ["orders"])
    suspend fun processMessage(record: ConsumerRecord<String, String>) {
        val correlationId = coroutineContext[CorrelationId]?.value // available here, and anywhere this suspends into
        ...
    }
}
```

## Spring Boot auto-configuration

Each `spring-kafka-kotlin-X.Y` module has a sibling `spring-kafka-kotlin-X.Y-boot` module: add it and a
`KafkaCoroutineTemplate`/`ReplyingKafkaCoroutineTemplate`/`RoutingKafkaCoroutineTemplate`/
`AggregatingReplyingKafkaCoroutineTemplate` bean is autowired for you from whatever `KafkaTemplate`/
`ReplyingKafkaTemplate`/`RoutingKafkaTemplate`/`AggregatingReplyingKafkaTemplate` bean you already have —
no manual construction needed. Back off by defining your own coroutine-template bean instead; it's picked
up in place of the auto-configured one.

To customize the `blockingIODispatcher` all four use, supply your own `CoroutineDispatcher` bean annotated
`@BlockingIODispatcher`:

```kotlin
@Bean
@BlockingIODispatcher
fun blockingIODispatcher(): CoroutineDispatcher = Dispatchers.IO.limitedParallelism(32)
```

`@BlockingIODispatcher` is this project's own qualifier annotation, not a Spring-provided one — Spring has
no built-in equivalent for "the dispatcher blocking coroutine work runs on" yet (see
[CLAUDE.md](CLAUDE.md) for the research behind that).

## Kotlin quality-of-life improvements

- **Null-safety** — signatures use Kotlin nullability instead of relying on
  `@Nullable`/javadoc convention, so misuse is a compile error, not an NPE.
- **`kotlin.time.Duration`** instead of `java.time.Duration` for timeout/delay
  parameters (e.g. `setCloseTimeout`, `receive`'s poll timeout) — units like
  `30.seconds` instead of `Duration.ofSeconds(30)`.
- **Exception transparency** — `try`/`catch` around a suspend call catches the
  real Kafka exception directly, with no `ExecutionException`/`CompletionException`
  to unwrap first (`.await()` already does that unwrapping internally).

## Non-goals

- **A Kotlin Reactive (Flow/Reactor) API.** This project provides coroutine-native
  abstractions over `spring-kafka` specifically — it isn't a reactive-streams layer. `reactor-kafka` (via Spring Boot's own reactive
  Kafka support) already covers that need for callers who want
  `Flux`/`Mono`/`kotlinx.coroutines.flow.Flow` semantics instead, as does
  [kotlin-kafka](https://github.com/nomisrev/kotlin-kafka), a `Flow`-native
  Kafka client built directly on `kafka-clients` rather than `spring-kafka`.
- **Wrapping `@KafkaListener`'s own `CompletableFuture` reply surface.** Recent
  `spring-kafka` versions let annotated listener methods return a
  `CompletableFuture` for async processing/replies — a second
  `CompletableFuture`-based surface alongside the four templates this project
  wraps. It's out of scope here for a structural reason, not an oversight:
  `@KafkaListener` methods are woven in by Spring's own container/proxying
  machinery, not a class this project can wrap or construct — there's no
  template object to substitute a coroutine-native version of. (See
  "A `CoWebFilter`-style hook for `@KafkaListener`" above for a related,
  narrower capability this project *does* provide.)

## Module layout

Pick the module matching the `spring-kafka` version already pulled in by your
Spring Boot version. Only one of these is meant to be on the classpath at a
time; all share the same package (`org.mdedetrich.spring.kafka.kotlin`).

Each module also has a sibling `-boot` module providing the auto-configuration
described above — depend on that instead if you want the coroutine templates
autowired rather than constructed by hand.

| Module | `-boot` module | `spring-kafka` version | Minimum JVM |
|---|---|---|---|
| `spring-kafka-kotlin-2.8` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-2.8)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-2.8) | `spring-kafka-kotlin-2.8-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-2.8-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-2.8-boot) | 2.8.11 | 8 |
| `spring-kafka-kotlin-2.9` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-2.9)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-2.9) | `spring-kafka-kotlin-2.9-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-2.9-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-2.9-boot) | 2.9.13 | 8 |
| `spring-kafka-kotlin-3.0` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.0)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.0) | `spring-kafka-kotlin-3.0-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.0-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.0-boot) | 3.0.15 | 17 |
| `spring-kafka-kotlin-3.1` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.1)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.1) | `spring-kafka-kotlin-3.1-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.1-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.1-boot) | 3.1.6 | 17 |
| `spring-kafka-kotlin-3.2` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.2)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.2) | `spring-kafka-kotlin-3.2-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.2-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.2-boot) | 3.2.10 | 17 |
| `spring-kafka-kotlin-3.3` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.3)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.3) | `spring-kafka-kotlin-3.3-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-3.3-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-3.3-boot) | 3.3.16 | 17 |
| `spring-kafka-kotlin-4.0` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-4.0)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-4.0) | `spring-kafka-kotlin-4.0-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-4.0-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-4.0-boot) | 4.0.6 | 17 |
| `spring-kafka-kotlin-4.1` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-4.1)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-4.1) | `spring-kafka-kotlin-4.1-boot` [![Maven Central](https://img.shields.io/maven-central/v/org.mdedetrich/spring-kafka-kotlin-4.1-boot)](https://central.sonatype.com/artifact/org.mdedetrich/spring-kafka-kotlin-4.1-boot) | 4.1.0 | 17 |

`spring-kafka` itself is declared `compileOnly` in each module (`provided`
scope) — the consuming application supplies it at runtime via its own
Spring Boot dependency management. Tests get the full dependency via
`testImplementation`.

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for why the repo is split this way.

## Building

```
./gradlew build
```

Runs compilation, tests, and `ktlintCheck` for every module.

```
./gradlew ktlintFormat
```

Auto-formats Kotlin sources and build scripts.
