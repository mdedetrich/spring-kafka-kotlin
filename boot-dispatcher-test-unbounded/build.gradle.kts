import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.spring)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-3.0-boot through 3.3-boot -- all 4 are on JUnit 6 (JVM 17 floor), which
// does execute suspend @Test methods directly (unlike boot-dispatcher-test-legacy's JUnit 5.14.4 floor).
// This module has no main source of its own -- only src/test, srcDir-shared into each consumer's own test
// source set.
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
// is entirely tests) -- spring-kafka-kotlin-3.0 and spring-boot-autoconfigure/spring-kafka 3.0's floor
// versions, the lowest of the 4 modules that share this source. Ignored/unused when srcDir'd into a real
// spring-kafka-kotlin-X.Y-boot module, which supplies its own versions on its own test classpath instead.
dependencies {
    testImplementation(project(":spring-kafka-kotlin-3.0"))
    testImplementation(project(":boot-autoconfiguration-unbounded"))
    testImplementation(libs.spring.kafka.v30)
    testImplementation(libs.spring.boot.autoconfigure.v30)
    testImplementation(libs.kotlinx.coroutines.core)
    // For AutoConfigurationOrderingTest's ApplicationContextRunner/AutoConfigurations.of(...) -- see that
    // test's own doc for why this exercises the real auto-configuration pipeline, unlike this module's
    // other tests.
    testImplementation(libs.spring.boot.test.v30)
    testImplementation(libs.assertj.core)

    testImplementation(platform(libs.junit.bom.jvm17))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
