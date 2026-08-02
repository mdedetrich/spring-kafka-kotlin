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

// Shared source, not a shared jar -- see boot-autoconfiguration-bounded/build.gradle.kts and
// boot-dispatcher-test-bounded/build.gradle.kts.
kotlin {
    sourceSets {
        main {
            kotlin.srcDir("$rootDir/boot-autoconfiguration-bounded/src/main/kotlin")
        }
        test {
            kotlin.srcDir("$rootDir/boot-dispatcher-test-bounded/src/test/kotlin")
        }
    }
}

dependencies {
    api(project(":spring-kafka-kotlin-4.0"))

    // Needed directly (not just transitively via the base module, which only depends on it as
    // `implementation`) to reference CoroutineDispatcher/Dispatchers in this module's own auto-configuration.
    implementation(libs.kotlinx.coroutines.core)

    // "provided" pattern, same as spring-kafka-kotlin-4.0 itself: the consuming Spring Boot application
    // supplies spring-boot-autoconfigure (via a starter) and spring-kafka (already required by the base
    // module) at runtime; this module only needs their types at compile time.
    compileOnly(libs.spring.boot.autoconfigure.v40)
    // KafkaAutoConfiguration lives here, not spring-boot-autoconfigure, from Boot 4.0 onward -- see the
    // catalog alias's own doc. Needed for @AutoConfigureAfter(KafkaAutoConfiguration::class), so this
    // auto-configuration is guaranteed to be processed after Boot's own (otherwise its
    // @ConditionalOnBean(KafkaTemplate::class) can evaluate before Boot's KafkaAutoConfiguration has
    // registered the KafkaTemplate bean at all -- confirmed via a real ApplicationContextRunner test).
    compileOnly(libs.spring.boot.kafka.v40)
    compileOnly(libs.spring.kafka.v40)
    // Generates META-INF/spring-autoconfigure-metadata.properties -- see the catalog alias's own doc.
    kapt(libs.spring.boot.autoconfigure.processor.v40)

    testImplementation(libs.spring.kafka.v40)
    testImplementation(libs.spring.boot.autoconfigure.v40)
    testImplementation(libs.spring.boot.kafka.v40)
    // Only for ImportCandidates in AutoConfigurationImportsTest -- see that alias's own doc in
    // libs.versions.toml for why this isn't a main-source dependency.
    testImplementation(libs.spring.boot.v40)
    // For AutoConfigurationOrderingTest's ApplicationContextRunner/AutoConfigurations.of(...) -- see that
    // test's own doc for why this exercises the real auto-configuration pipeline, unlike this module's
    // other tests.
    testImplementation(libs.spring.boot.test.v40)
    testImplementation(libs.assertj.core)

    // JUnit 6 requires JVM 17+; safe here since this module already targets JVM 17.
    testImplementation(platform(libs.junit.bom.jvm17))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation(kotlin("test"))
}
repositories {
    mavenCentral()
}
