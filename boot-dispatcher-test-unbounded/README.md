# boot-dispatcher-test-unbounded

Shared `KafkaCoroutineTemplateAutoConfigurationTest`/`ReplyingKafkaCoroutineTemplateAutoConfigurationTest`/
`RoutingKafkaCoroutineTemplateAutoConfigurationTest`/
`AggregatingReplyingKafkaCoroutineTemplateAutoConfigurationTest` (verify the `blockingIODispatcher`
qualifier/`ObjectProvider` wiring end to end, via `ThreadRecordingKafkaTemplate`/
`ThreadRecordingReplyingKafkaTemplate`/`ThreadRecordingRoutingKafkaTemplate`/
`ThreadRecordingAggregatingReplyingKafkaTemplate` fixtures), defined identically across
`spring-kafka-kotlin-3.0-boot` through `3.3-boot` -- **not** a dependency any of them declare via
`project(":boot-dispatcher-test-unbounded")`. Each consuming module's `build.gradle.kts` instead adds this
module's `src/test/kotlin` directly as an extra `kotlin.srcDir` on its own `test` source set:

```kotlin
kotlin {
    sourceSets {
        test {
            kotlin.srcDir("$rootDir/boot-dispatcher-test-unbounded/src/test/kotlin")
        }
    }
}
```

So these files get compiled and run independently in each consuming module, against that module's own
spring-boot-autoconfigure/spring-kafka version and classpath -- no shared jar.

## Why this variant exists separately

`spring-kafka-kotlin-3.0-boot` through `3.3-boot` are on `junit-bom-jvm17` (JUnit 6), which executes
`suspend fun` `@Test` methods directly -- unlike
[boot-dispatcher-test-legacy](../boot-dispatcher-test-legacy/README.md)'s JUnit 5.14.4 floor, which
silently never invokes them. `spring-kafka-kotlin-4.0-boot`/`4.1-boot` are also on JUnit 6 but need a
different `MockProducer` constructor call (spring-kafka 4.0+'s kafka-clients pairing requires the 4-arg
overload with an explicit `null` `Partitioner`) -- see
[boot-dispatcher-test-bounded](../boot-dispatcher-test-bounded/README.md).
