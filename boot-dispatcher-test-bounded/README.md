# boot-dispatcher-test-bounded

Shared `KafkaCoroutineTemplateAutoConfigurationTest`/`ReplyingKafkaCoroutineTemplateAutoConfigurationTest`/
`RoutingKafkaCoroutineTemplateAutoConfigurationTest`/
`AggregatingReplyingKafkaCoroutineTemplateAutoConfigurationTest` (verify the `blockingIODispatcher`
qualifier/`ObjectProvider` wiring end to end, via `ThreadRecordingKafkaTemplate`/
`ThreadRecordingReplyingKafkaTemplate`/`ThreadRecordingRoutingKafkaTemplate`/
`ThreadRecordingAggregatingReplyingKafkaTemplate` fixtures), defined identically across
`spring-kafka-kotlin-4.0-boot` and `4.1-boot` -- **not** a dependency either declares via
`project(":boot-dispatcher-test-bounded")`. Each consuming module's `build.gradle.kts` instead adds this
module's `src/test/kotlin` directly as an extra `kotlin.srcDir` on its own `test` source set:

```kotlin
kotlin {
    sourceSets {
        test {
            kotlin.srcDir("$rootDir/boot-dispatcher-test-bounded/src/test/kotlin")
        }
    }
}
```

So these files get compiled and run independently in each consuming module, against that module's own
spring-boot-autoconfigure/spring-kafka version and classpath -- no shared jar.

## Why this variant exists separately

Identical to [boot-dispatcher-test-unbounded](../boot-dispatcher-test-unbounded/README.md) except for one
line: spring-kafka 4.0+'s kafka-clients pairing requires `MockProducer`'s 4-arg constructor overload with
an explicit `null` `Partitioner`, where the pre-4.0 pairing accepts a 3-arg overload without it.
