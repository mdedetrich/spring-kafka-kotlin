# boot-autoconfiguration-bounded

Shared `KafkaCoroutineTemplateAutoConfiguration`/`ReplyingKafkaCoroutineTemplateAutoConfiguration`/
`RoutingKafkaCoroutineTemplateAutoConfiguration`/`AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration`
(the `@Configuration(proxyBeanMethods = false)` classes that expose autowirable `KafkaCoroutineTemplate`/
`ReplyingKafkaCoroutineTemplate`/`RoutingKafkaCoroutineTemplate`/`AggregatingReplyingKafkaCoroutineTemplate`
beans), defined identically across `spring-kafka-kotlin-4.0-boot` and `4.1-boot` -- **not** a dependency
either declares via
`project(":boot-autoconfiguration-bounded")`. Each consuming module's `build.gradle.kts` instead adds this
module's `src/main/kotlin` directly as an extra `kotlin.srcDir` on its own `main` source set, same pattern
as `hook-aspect-native`/`send-and-receive-result-bounded`/etc.:

```kotlin
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/boot-autoconfiguration-bounded/src/main/kotlin")
        }
    }
}
```

So these files get compiled independently into each consuming module, against that module's own
spring-boot-autoconfigure/spring-kafka version and classpath -- no shared jar, no runtime dependency on
this module at all.

## Why there's a `-bounded`/`-unbounded` split

See [boot-autoconfiguration-unbounded](../boot-autoconfiguration-unbounded/README.md) -- the
`KafkaCoroutineTemplateAutoConfiguration`/`ReplyingKafkaCoroutineTemplateAutoConfiguration`/
`AggregatingReplyingKafkaCoroutineTemplateAutoConfiguration` `@Bean` methods here use
`<K : Any, V : Any>`/`<K : Any, V : Any, R : Any>`, matching the bounds spring-kafka 4.0+'s JSpecify
`@NullMarked` packages force on the base modules' own template classes. Both consumers already target JVM
17, so no lower floor to accommodate here. `RoutingKafkaCoroutineTemplateAutoConfiguration` is the exception
-- `RoutingKafkaTemplate` is fixed to `KafkaTemplate<Object, Object>` (not generic), so that file is
byte-identical to the copy in `boot-autoconfiguration-unbounded`, placed here unchanged rather than needing
its own bounded variant. `BlockingIODispatcher.kt` (the qualifier annotation, see below) is likewise
byte-identical in both modules -- it has no generics of its own either.

## `@BlockingIODispatcher` qualifier

See [boot-autoconfiguration-unbounded](../boot-autoconfiguration-unbounded/README.md#blockingiodispatcher-qualifier)
-- identical reasoning and identical file, this variant only differs in the auto-configuration `@Bean`
methods' own generic bounds.
