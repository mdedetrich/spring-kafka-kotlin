import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    // Bundled with kotlin-gradle-plugin itself -- no separate version to pin via the catalog.
    id("org.jetbrains.kotlin.kapt")
    alias(libs.plugins.kotlin.plugin.spring)
    alias(libs.plugins.ktlint)
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

// Shared source, not a shared jar -- see boot-autoconfiguration-unbounded/build.gradle.kts and
// boot-dispatcher-test-unbounded/build.gradle.kts.
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/boot-autoconfiguration-unbounded/src/main/kotlin")
        }
        test {
            kotlin.srcDir("$rootDir/boot-dispatcher-test-unbounded/src/test/kotlin")
        }
    }
}

dependencies {
    api(project(":spring-kafka-kotlin-3.2"))

    // Needed directly (not just transitively via the base module, which only depends on it as
    // `implementation`) to reference CoroutineDispatcher/Dispatchers in this module's own auto-configuration.
    implementation(libs.kotlinx.coroutines.core)

    // "provided" pattern, same as spring-kafka-kotlin-3.2 itself: the consuming Spring Boot application
    // supplies spring-boot-autoconfigure (via a starter) and spring-kafka (already required by the base
    // module) at runtime; this module only needs their types at compile time.
    compileOnly(libs.spring.boot.autoconfigure.v32)
    compileOnly(libs.spring.kafka.v32)
    // Generates META-INF/spring-autoconfigure-metadata.properties -- see the catalog alias's own doc.
    kapt(libs.spring.boot.autoconfigure.processor.v32)

    testImplementation(libs.spring.kafka.v32)
    testImplementation(libs.spring.boot.autoconfigure.v32)
    // Only for ImportCandidates in AutoConfigurationImportsTest -- see that alias's own doc in
    // libs.versions.toml for why this isn't a main-source dependency.
    testImplementation(libs.spring.boot.v32)
    // For AutoConfigurationOrderingTest's ApplicationContextRunner/AutoConfigurations.of(...) -- see that
    // test's own doc for why this exercises the real auto-configuration pipeline, unlike this module's
    // other tests.
    testImplementation(libs.spring.boot.test.v32)
    testImplementation(libs.assertj.core)

    // JUnit 6 requires JVM 17+; safe here since this module already targets JVM 17.
    testImplementation(platform(libs.junit.bom.jvm17))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
