# boot-dispatcher-test-legacy

Shared `KafkaCoroutineTemplateAutoConfigurationTest`/`ReplyingKafkaCoroutineTemplateAutoConfigurationTest`/
`RoutingKafkaCoroutineTemplateAutoConfigurationTest`/
`AggregatingReplyingKafkaCoroutineTemplateAutoConfigurationTest` (verify the `blockingIODispatcher`
qualifier/`ObjectProvider` wiring end to end, via `ThreadRecordingKafkaTemplate`/
`ThreadRecordingReplyingKafkaTemplate`/`ThreadRecordingRoutingKafkaTemplate`/
`ThreadRecordingAggregatingReplyingKafkaTemplate` fixtures), defined identically across
`spring-kafka-kotlin-2.8-boot` and `2.9-boot` -- **not** a dependency either declares via
`project(":boot-dispatcher-test-legacy")`. Each consuming module's `build.gradle.kts` instead adds this
module's `src/test/kotlin` directly as an extra `kotlin.srcDir` on its own `test` source set:

```kotlin
kotlin {
    sourceSets {
        test {
            kotlin.srcDir("$rootDir/boot-dispatcher-test-legacy/src/test/kotlin")
        }
    }
}
```

So these files get compiled and run independently in each consuming module, against that module's own
spring-boot-autoconfigure/spring-kafka version and classpath -- no shared jar. This is the first shared
module in this repo to share `src/test` rather than `src/main`; mechanically identical (Gradle doesn't
distinguish), the difference is only which source set gets the extra `srcDir`.

## Why this variant exists separately

`spring-kafka-kotlin-2.8-boot`/`2.9-boot` are on JUnit 5.14.4 (their JVM 8 floor via `junit-bom-jvm8`),
which does not execute `suspend fun` `@Test` methods at all -- they're silently never invoked, not failed,
which is easy to miss. This variant uses plain `fun` + `runBlocking { }` instead, matching the same fix
already used in `spring-kafka-kotlin-2.8`/`2.9`'s own `KafkaCoroutineTemplateReceiveTest`.
`spring-kafka-kotlin-3.0-boot` onward use `junit-bom-jvm17` (JUnit 6), which does support `suspend fun`
tests directly -- see [boot-dispatcher-test-unbounded](../boot-dispatcher-test-unbounded/README.md).
