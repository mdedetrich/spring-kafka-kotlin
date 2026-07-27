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
    "spring-kafka-kotlin-2.8",
    "spring-kafka-kotlin-2.9",
    "spring-kafka-kotlin-3.0",
    "spring-kafka-kotlin-3.1",
    "spring-kafka-kotlin-3.2",
    "spring-kafka-kotlin-3.3",
    "spring-kafka-kotlin-4.0",
    "spring-kafka-kotlin-4.1",
    "benchmarks",
    "benchmarks-compat",
)
