# send-and-receive-result-unbounded

Shared `SendAndReceiveResult` data class and its 4 reified `sendAndReceiveTyped`/`sendAndReceiveDeferredTyped`
extension functions on `ReplyingKafkaCoroutineOperations`, defined identically across
`spring-kafka-kotlin-2.8` through `3.3` -- **not** a dependency any of them declare via
`project(":send-and-receive-result-unbounded")`. Each consuming module's `build.gradle.kts` instead adds
this module's `src/main/kotlin` directly as an extra `kotlin.srcDir` on its own `main` source set, same
pattern as `hook-aspect-native`/`hook-aspect-compat`/`micrometer-tags-provider-cache`:

```kotlin
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/send-and-receive-result-unbounded/src/main/kotlin")
        }
    }
}
```

So files placed here get compiled independently into each consuming module, against that module's own
`spring-kafka` version and classpath -- no shared jar, no runtime dependency on this module at all.

## Why there's a `-bounded`/`-unbounded` split

spring-kafka 4.0 marks `org.springframework.kafka.core`/`.support`/`.requestreply` `@NullMarked`
(JSpecify), which forces the Kotlin-side `K`/`V`/`R` type parameters used by `SendAndReceiveResult` and the
reified extensions to be bound to `Any` from that version onward -- an unbounded `SendAndReceiveResult<K,
V, R>` no longer type-checks against the now-non-null Java generics. `spring-kafka-kotlin-2.8` through
`3.3` predate that annotation and keep the type parameters unbounded, so this module holds that variant;
`spring-kafka-kotlin-4.0` and `4.1` use the `<K : Any, V : Any, R : Any>` variant in
[send-and-receive-result-bounded](../send-and-receive-result-bounded/README.md) instead.

2.8 is the strictest floor (JVM 8) across the 6 consumers of this module -- code added here must stay
compatible with it, since it's compiled directly into every one of them, not depended on as a jar.
