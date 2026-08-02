import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.spring)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-2.8-boot and 2.9-boot only -- both are on JUnit 5.14.4 (their JVM 8
// floor), which doesn't execute suspend @Test methods, so this variant uses plain fun + runBlocking
// instead (see the test file's own note). This module has no main source of its own -- only
// src/test, srcDir-shared into each consumer's own test source set.
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_1_8)
    }
}

// Needed for this module's own standalone test execution (not just compilation, since this source set
// is entirely tests) -- spring-kafka-kotlin-2.8 and spring-boot-autoconfigure/spring-kafka 2.8's floor
// versions, the lowest of the 2 modules that share this source. Ignored/unused when srcDir'd into a real
// spring-kafka-kotlin-X.Y-boot module, which supplies its own versions on its own test classpath instead.
dependencies {
    testImplementation(project(":spring-kafka-kotlin-2.8"))
    testImplementation(project(":boot-autoconfiguration-unbounded"))
    testImplementation(libs.spring.kafka.v28)
    testImplementation(libs.spring.boot.autoconfigure.v28)
    testImplementation(libs.kotlinx.coroutines.core)
    // For AutoConfigurationOrderingTest's ApplicationContextRunner/AutoConfigurations.of(...) -- see that
    // test's own doc for why this exercises the real auto-configuration pipeline, unlike this module's
    // other tests.
    testImplementation(libs.spring.boot.test.v28)
    testImplementation(libs.assertj.core)

    testImplementation(platform(libs.junit.bom.jvm8))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
