# micrometer-tags-provider-cache

Shared `MicrometerTagsProviderCache` used by all four coroutine templates
(`KafkaCoroutineTemplate`, `ReplyingKafkaCoroutineTemplate`, `RoutingKafkaCoroutineTemplate`,
`AggregatingReplyingKafkaCoroutineTemplate`), shared across `spring-kafka-kotlin-2.9` through `4.1` --
**not** a dependency any of them declare via `project(":micrometer-tags-provider-cache")`. Each applicable
module's `build.gradle.kts` instead adds this module's `src/main/kotlin` directly as an extra
`kotlin.srcDir` on its own `main` source set, same pattern as `hook-aspect-native`/`hook-aspect-compat`:

```kotlin
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/micrometer-tags-provider-cache/src/main/kotlin")
        }
    }
}
```

So files placed here get compiled independently into each consuming module, against that module's own
`spring-kafka` version and classpath -- no shared jar, no runtime dependency on this module at all. The
class stays `internal`: once srcDir-included, it's part of that module's own compilation, so `internal`
visibility resolves normally against that module's other classes (e.g. `KafkaCoroutineTemplate.kt` in the
same module).

## Why `spring-kafka-kotlin-2.8` is excluded

`KafkaTemplate.getMicrometerTagsProvider()`/`setMicrometerTagsProvider(...)` don't exist on the real
`KafkaTemplate.java` shipped in spring-kafka 2.8.11 -- confirmed by downloading and inspecting that
version's actual sources directly, not assumed from the version number. They're present from spring-kafka
2.9.13 onward (verified the same way across 2.9.13, 3.0.15, 3.1.6, 3.2.10, 3.3.16, 4.0.6, and 4.1.0,
including across the 4.0 major-version bump), so `spring-kafka-kotlin-2.8` gets none of this module's
wiring and keeps no `MicrometerTagsProviderCache` at all.
