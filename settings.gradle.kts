pluginManagement {
    includeBuild("build-logic")
}

rootProject.name = "spring-kafka-kotlin"

include(
    "hook-aspect-native",
    "hook-aspect-compat",
    "micrometer-tags-provider-cache",
    "send-and-receive-result-unbounded",
    "send-and-receive-result-bounded",
    "boot-autoconfiguration-unbounded",
    "boot-autoconfiguration-bounded",
    "boot-dispatcher-test-legacy",
    "boot-dispatcher-test-unbounded",
    "boot-dispatcher-test-bounded",
    "spring-kafka-kotlin-2.8",
    "spring-kafka-kotlin-2.9",
    "spring-kafka-kotlin-3.0",
    "spring-kafka-kotlin-3.1",
    "spring-kafka-kotlin-3.2",
    "spring-kafka-kotlin-3.3",
    "spring-kafka-kotlin-4.0",
    "spring-kafka-kotlin-4.1",
    "spring-kafka-kotlin-2.8-boot",
    "spring-kafka-kotlin-2.9-boot",
    "spring-kafka-kotlin-3.0-boot",
    "spring-kafka-kotlin-3.1-boot",
    "spring-kafka-kotlin-3.2-boot",
    "spring-kafka-kotlin-3.3-boot",
    "spring-kafka-kotlin-4.0-boot",
    "spring-kafka-kotlin-4.1-boot",
    "benchmarks",
    "benchmarks-compat",
)
