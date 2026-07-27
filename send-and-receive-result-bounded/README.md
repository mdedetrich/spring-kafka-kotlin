# send-and-receive-result-bounded

Shared `SendAndReceiveResult` data class and its 4 reified `sendAndReceiveTyped`/`sendAndReceiveDeferredTyped`
extension functions on `ReplyingKafkaCoroutineOperations`, defined identically across
`spring-kafka-kotlin-4.0` and `4.1` -- **not** a dependency either declares via
`project(":send-and-receive-result-bounded")`. Each consuming module's `build.gradle.kts` instead adds
this module's `src/main/kotlin` directly as an extra `kotlin.srcDir` on its own `main` source set, same
pattern as `hook-aspect-native`/`hook-aspect-compat`/`micrometer-tags-provider-cache`:

```kotlin
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/send-and-receive-result-bounded/src/main/kotlin")
        }
    }
}
```

So files placed here get compiled independently into each consuming module, against that module's own
`spring-kafka` version and classpath -- no shared jar, no runtime dependency on this module at all.

## Why there's a `-bounded`/`-unbounded` split

spring-kafka 4.0 marks `org.springframework.kafka.core`/`.support`/`.requestreply` `@NullMarked`
(JSpecify), which forces the Kotlin-side `K`/`V`/`R` type parameters used by `SendAndReceiveResult` and the
reified extensions to be bound to `Any` -- an unbounded `SendAndReceiveResult<K, V, R>` no longer
type-checks against the now-non-null Java generics. This module holds that `<K : Any, V : Any, R : Any>`
variant for `spring-kafka-kotlin-4.0` and `4.1`; the pre-4.0 unbounded variant used by
`spring-kafka-kotlin-2.8` through `3.3` lives in
[send-and-receive-result-unbounded](../send-and-receive-result-unbounded/README.md) instead.

Both consumers already target JVM 17, so no lower floor to accommodate here.
