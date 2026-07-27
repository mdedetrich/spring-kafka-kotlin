import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
    alias(libs.plugins.kotlin.plugin.spring)
    id("spring-kafka-kotlin.published-module")
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

// Shared source, not a shared jar -- see hook-aspect-native/README.md,
// micrometer-tags-provider-cache/README.md and send-and-receive-result-unbounded/README.md.
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/hook-aspect-native/src/main/kotlin")
            kotlin.srcDir("$rootDir/micrometer-tags-provider-cache/src/main/kotlin")
            kotlin.srcDir("$rootDir/send-and-receive-result-unbounded/src/main/kotlin")
        }
    }
}

dependencies {
    compileOnly(libs.spring.kafka.v32)
    testImplementation(libs.spring.kafka.v32)

    // For the @KafkaListener coroutine-dispatch Aspect (hook-aspect-native): provided by the consuming
    // application, same "provided" pattern as spring-kafka itself -- a real Spring Boot app normally
    // gets both via spring-boot-starter-aop.
    compileOnly(libs.spring.aop.v32plus)
    compileOnly(libs.aspectjweaver)
    testImplementation(libs.spring.aop.v32plus)
    testImplementation(libs.aspectjweaver)

    implementation(libs.kotlinx.coroutines.jdk8)

    // Needed at runtime (not just in tests) to detect and invoke suspend @KafkaListener methods from the
    // coroutine-dispatch Aspect.
    implementation(kotlin("reflect"))

    // JUnit 6 requires JVM 17+; safe here since this module already targets JVM 17.
    testImplementation(platform(libs.junit.bom.jvm17))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(kotlin("test"))
}
repositories {
    mavenCentral()
}
