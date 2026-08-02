# boot-autoconfiguration-unbounded

Shared `KafkaCoroutineTemplateAutoConfiguration`/`ReplyingKafkaCoroutineTemplateAutoConfiguration`/
`RoutingKafkaCoroutineTemplateAutoConfiguration`/`AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration`
(the `@Configuration(proxyBeanMethods = false)` classes that expose autowirable `KafkaCoroutineTemplate`/
`ReplyingKafkaCoroutineTemplate`/`RoutingKafkaCoroutineTemplate`/`AggregatingReplyingKafkaCoroutineTemplate`
beans), defined identically across `spring-kafka-kotlin-2.8-boot` through `3.3-boot` -- **not** a
dependency any of them declare via
`project(":boot-autoconfiguration-unbounded")`. Each consuming module's `build.gradle.kts` instead adds
this module's `src/main/kotlin` directly as an extra `kotlin.srcDir` on its own `main` source set, same
pattern as `hook-aspect-native`/`send-and-receive-result-unbounded`/etc.:

```kotlin
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/boot-autoconfiguration-unbounded/src/main/kotlin")
        }
    }
}
```

So these files get compiled independently into each consuming module, against that module's own
spring-boot-autoconfigure/spring-kafka version and classpath -- no shared jar, no runtime dependency on
this module at all.

`ReplyingKafkaCoroutineTemplateAutoConfiguration` is gated on `@ConditionalOnBean(ReplyingKafkaTemplate::class)`
rather than `KafkaTemplate` -- unlike `KafkaTemplate`, Spring Boot's own `KafkaAutoConfiguration` never
provides a `ReplyingKafkaTemplate` bean automatically (request/reply needs an application-specific reply
container/topic), so it only activates once the application has registered its own. Same story for
`RoutingKafkaCoroutineTemplateAutoConfiguration` (`@ConditionalOnBean(RoutingKafkaTemplate::class)` --
routing needs an application-specific topic-pattern-to-`ProducerFactory` map), except `RoutingKafkaTemplate`
is fixed to `KafkaTemplate<Object, Object>` (not generic), so that one `@Bean` method has no type
parameters at all and its source file is identical in both this module and
[boot-autoconfiguration-bounded](../boot-autoconfiguration-bounded/README.md) -- placed in both unchanged,
rather than needing two different variants.
`AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration` mirrors
`ReplyingKafkaCoroutineTemplateAutoConfiguration` almost exactly (`AggregatingReplyingKafkaTemplate`
extends `ReplyingKafkaTemplate`) -- gated on `@ConditionalOnBean(AggregatingReplyingKafkaTemplate::class)`
for the same "Boot never provides one automatically" reason.

## Why there's a `-bounded`/`-unbounded` split

Matches the identical split already established for `KafkaCoroutineTemplate` itself (see
[send-and-receive-result-unbounded](../send-and-receive-result-unbounded/README.md)): spring-kafka 4.0's
JSpecify `@NullMarked` packages force `KafkaCoroutineTemplate<K : Any, V : Any>` from that version onward,
so this auto-configuration's own `@Bean` method signature has to match. `spring-kafka-kotlin-2.8-boot`
through `3.3-boot` (6 consumers) use this unbounded variant; `4.0-boot`/`4.1-boot` use the bounded variant
in [boot-autoconfiguration-bounded](../boot-autoconfiguration-bounded/README.md) instead.

2.8-boot is the strictest floor (JVM 8) across the 6 consumers of this module -- code added here must stay
compatible with it, since it's compiled directly into every one of them, not depended on as a jar.

## `@BlockingIODispatcher` qualifier

The optional `CoroutineDispatcher` bean all four of `kafkaCoroutineTemplate`,
`replyingKafkaCoroutineTemplate`, `routingKafkaCoroutineTemplate` and
`aggregatingReplyingKafkaCoroutineTemplate` pass through as `blockingIODispatcher` is resolved via
`ObjectProvider<CoroutineDispatcher>`, scoped with a dedicated `@BlockingIODispatcher` marker annotation
(defined in this module's own `BlockingIODispatcher.kt`, meta-annotated `@Qualifier`) rather than matching
any `CoroutineDispatcher` bean in the context -- the same annotation is reused by all four `@Bean` methods,
since it's one dispatcher for "blocking Kafka coroutine wrapper calls run on", not a separate concept per
template class.

This mirrors Spring Boot's own `@FlywayDataSource`/`@BatchDataSource`/`@QuartzDataSource`/
`@LiquibaseDataSource` -- a tiny marker annotation meta-annotated `@Qualifier`, not a raw string-literal
qualifier value -- for the identical problem shape (a common, reusable bean type where a specific one
needs to be injected, and Spring's own reference docs call out that qualifier values should be short
semantic words like `main`/`EMEA`/`persistent`, not bean-name-style compound strings). Being Kotlin-authored
(unlike `@FlywayDataSource`, a `.java` file), it uses
`@Target(AnnotationTarget.VALUE_PARAMETER, AnnotationTarget.FUNCTION, AnnotationTarget.FIELD)`/
`@Retention(AnnotationRetention.RUNTIME)` rather than the `java.lang.annotation` equivalents -- confirmed
against Spring Framework's own Kotlin reference docs, which show precisely this translation (a `@Genre`
example, itself using `FIELD`/`VALUE_PARAMETER`) for a custom qualifier authored in Kotlin. All three
targets are load-bearing, verified by actually compiling each shape: `VALUE_PARAMETER` for the
auto-configuration's own consuming parameter (also covers a consumer's constructor-`val`/setter-parameter
injection of the same qualified bean), `FUNCTION` for the `@Bean` methods in `boot-dispatcher-test-*` that
supply the qualified dispatcher in tests, and `FIELD` for a consumer reusing the same qualified bean via
plain field injection elsewhere in their own app (`@Autowired @BlockingIODispatcher lateinit var dispatcher: CoroutineDispatcher`)
-- omitting `FIELD` fails to compile with "this annotation is not applicable to target 'member property
with backing field'".

Falls back to `Dispatchers.IO` (the class's own default), not `null` -- `null` is itself a meaningful value
on `KafkaCoroutineTemplate` ("run on the caller's dispatcher"), not merely "unconfigured".
