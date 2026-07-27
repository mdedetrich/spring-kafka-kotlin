import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
    alias(libs.plugins.kotlin.plugin.allopen)
    alias(libs.plugins.kotlinx.benchmark)
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

// JMH generates a subclass per @State class to run benchmarks against -- Kotlin classes are final by
// default, so anything annotated @State (and its @Benchmark members) needs to be open.
allOpen {
    annotation("org.openjdk.jmh.annotations.State")
}

dependencies {
    implementation(project(":spring-kafka-kotlin-3.3"))

    // spring-kafka-kotlin-3.3 declares these compileOnly (the consuming application is expected to
    // supply them) -- this module plays that role, same as spring-kafka-kotlin-3.3's own test source set.
    implementation(libs.spring.kafka.v33)
    implementation(libs.spring.aop.v32plus)
    implementation(libs.aspectjweaver)

    // spring-kafka-kotlin-3.3 depends on this as `implementation`, which isn't exposed on a project
    // dependency's compile classpath -- needed here directly for runBlocking.
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
