import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-2.8-boot through 3.3-boot -- the unbounded generics group (see
// KafkaCoroutineTemplate's own unbounded/bounded split in the base modules for the same root cause).
// 2.8-boot is the lowest-JVM consumer, so this module targets JVM 8 too, to genuinely verify the shared
// source is JVM 8-compatible rather than relying on a higher-JVM consumer to mask an incompatibility.
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_1_8)
    }
}

// These dependencies are ignored/unused when this source is compiled as part of a real
// spring-kafka-kotlin-X.Y-boot module (via sourceSets.main.kotlin.srcDir) -- each of those 6 modules
// supplies its own spring-boot-autoconfigure/spring-kafka versions on its own compileKotlin classpath
// instead. They only matter for this module's own standalone compileKotlin, which also compiles this same
// file (its own main source set, not just srcDir-included elsewhere) -- spring-boot-autoconfigure 2.6.15
// and spring-kafka 2.8.11, the floor versions across the 6 modules that share this source. The
// compileOnly project dependency is purely so KafkaCoroutineTemplate resolves at compile time here too --
// not a real runtime dependency, and not a cycle (srcDir sharing doesn't create a reverse project
// dependency), same reasoning as hook-aspect-native's own equivalent dependency.
dependencies {
    compileOnly(project(":spring-kafka-kotlin-2.8"))
    compileOnly(libs.spring.boot.autoconfigure.v28)
    compileOnly(libs.spring.kafka.v28)
    implementation(libs.kotlinx.coroutines.core)
}
