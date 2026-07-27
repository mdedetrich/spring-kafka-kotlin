import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
}

// Shared only with spring-kafka-kotlin-3.2 and later -- see README.md for why: suspend @KafkaListener
// support (KotlinAwareInvocableHandlerMethod) doesn't exist in spring-kafka before 3.2, so this source
// isn't valid/functional on 2.8/2.9/3.0/3.1 regardless of whether it compiles there. All 4 consuming
// modules already target JVM 17, so no lower floor to accommodate here.
java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<KotlinCompile>().configureEach {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

// These dependencies are ignored/unused when this source is compiled as part of a real
// spring-kafka-kotlin-* module (via sourceSets.main.kotlin.srcDir) -- each of those 4 modules supplies
// its own spring-kafka/spring-aop versions on its own compileKotlin classpath instead. They only matter
// for this module's own standalone compileKotlin, which also compiles these same files (they're its own
// main source set, not just srcDir-included elsewhere) -- spring-kafka:3.2.10, the floor version across
// the 4 modules that share this source.
dependencies {
    compileOnly(libs.spring.kafka.v32)
    compileOnly(libs.spring.aop.v32plus)
    compileOnly(libs.aspectjweaver)

    implementation(libs.kotlinx.coroutines.jdk8)
    implementation(kotlin("reflect"))
}
