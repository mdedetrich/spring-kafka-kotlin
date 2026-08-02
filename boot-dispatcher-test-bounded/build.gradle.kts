import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.spring)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-4.0-boot and 4.1-boot -- differs from boot-dispatcher-test-unbounded
// only in MockProducer's constructor shape (spring-kafka 4.0+'s kafka-clients pairing requires the 4-arg
// overload with an explicit null Partitioner). This module has no main source of its own -- only
// src/test, srcDir-shared into each consumer's own test source set.
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

// Needed for this module's own standalone test execution (not just compilation, since this source set
// is entirely tests) -- spring-kafka-kotlin-4.0 and spring-boot-autoconfigure/spring-kafka 4.0's floor
// versions, the lowest of the 2 modules that share this source. Ignored/unused when srcDir'd into a real
// spring-kafka-kotlin-X.Y-boot module, which supplies its own versions on its own test classpath instead.
dependencies {
    testImplementation(project(":spring-kafka-kotlin-4.0"))
    testImplementation(project(":boot-autoconfiguration-bounded"))
    testImplementation(libs.spring.kafka.v40)
    testImplementation(libs.spring.boot.autoconfigure.v40)
    testImplementation(libs.spring.boot.kafka.v40)
    testImplementation(libs.kotlinx.coroutines.core)
    // For AutoConfigurationOrderingTest's ApplicationContextRunner/AutoConfigurations.of(...) -- see that
    // test's own doc for why this exercises the real auto-configuration pipeline, unlike this module's
    // other tests.
    testImplementation(libs.spring.boot.test.v40)
    testImplementation(libs.assertj.core)

    testImplementation(platform(libs.junit.bom.jvm17))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
