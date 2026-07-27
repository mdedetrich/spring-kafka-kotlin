import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
    alias(libs.plugins.kotlin.plugin.allopen)
    alias(libs.plugins.kotlinx.benchmark)
}

// Matches hook-aspect-compat's own floor -- deliberately not combined into the main `benchmarks` module:
// hook-aspect-native and hook-aspect-compat both compile classes under the exact same package and class
// names (by design, for public API parity), so having both on one classpath at once would collide --
// exactly the same "only one spring-kafka-kotlin-* module on the classpath at a time" constraint this
// project already documents, just applying to its own benchmarks too.
java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_1_8)
    }
}

// JMH generates a subclass per @State class to run benchmarks against -- Kotlin classes are final by
// default, so anything annotated @State (and its @Benchmark members) needs to be open.
allOpen {
    annotation("org.openjdk.jmh.annotations.State")
}

dependencies {
    implementation(project(":hook-aspect-compat"))

    // hook-aspect-compat declares these compileOnly (the consuming application is expected to supply
    // them) -- this module plays that role, same as its own test source set. spring-kafka:2.8.11 (and
    // its actual Spring Framework 5.3.25) is the floor across the 4 modules this backport targets.
    implementation(libs.spring.kafka.v28)
    implementation(libs.spring.aop.v28)
    implementation(libs.aspectjweaver)

    implementation(libs.kotlinx.coroutines.core)

    implementation(libs.kotlinx.benchmark.runtime)
}

benchmark {
    targets {
        register("main")
    }
}

repositories {
    mavenCentral()
}
