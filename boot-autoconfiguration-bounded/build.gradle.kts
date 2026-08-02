import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-4.0-boot and 4.1-boot -- the bounded generics group (spring-kafka 4.0+'s
// JSpecify @NullMarked packages force KafkaCoroutineTemplate<K : Any, V : Any> in the base modules; this
// auto-configuration's own @Bean method signature has to match). Both consumers already target JVM 17, so
// no lower floor to accommodate here.
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
// spring-kafka-kotlin-X.Y-boot module (via sourceSets.main.kotlin.srcDir) -- each of those 2 modules
// supplies its own spring-boot-autoconfigure/spring-kafka versions on its own compileKotlin classpath
// instead. They only matter for this module's own standalone compileKotlin -- spring-boot-autoconfigure
// 4.0.7 and spring-kafka 4.0.6, the floor versions across the 2 modules that share this source. The
// compileOnly project dependency is purely so KafkaCoroutineTemplate resolves at compile time here too --
// not a real runtime dependency, and not a cycle, same reasoning as hook-aspect-native's own equivalent
// dependency.
dependencies {
    compileOnly(project(":spring-kafka-kotlin-4.0"))
    compileOnly(libs.spring.boot.autoconfigure.v40)
    // KafkaAutoConfiguration lives here, not spring-boot-autoconfigure, from Boot 4.0 onward -- see the
    // catalog alias's own doc.
    compileOnly(libs.spring.boot.kafka.v40)
    compileOnly(libs.spring.kafka.v40)
    implementation(libs.kotlinx.coroutines.core)
}
