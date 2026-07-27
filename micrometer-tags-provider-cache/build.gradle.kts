import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-2.9 through 4.1 (not 2.8: KafkaTemplate.micrometerTagsProvider doesn't
// exist there yet) -- see README.md. 2.9 is the strictest floor (JVM 8) across those 7 consumers -- code
// added here must stay compatible with it, since it's compiled directly into every one of them via
// sourceSets.main.kotlin.srcDir, not depended on as a jar.
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
// spring-kafka-kotlin-* module (via sourceSets.main.kotlin.srcDir) -- each of those 7 modules supplies
// its own spring-kafka version on its own compileKotlin classpath instead. They only matter for this
// module's own standalone compileKotlin/test, which also compiles/runs against these same files --
// spring-kafka 2.9.13 is the floor across the 7 modules that share this source, and deliberately the
// version tested against here.
dependencies {
    compileOnly(libs.spring.kafka.v29)
    testImplementation(libs.spring.kafka.v29)

    // JUnit 6 requires JVM 17+, incompatible with this module's JVM 8 target; latest 5.x instead.
    testImplementation(platform(libs.junit.bom.jvm8))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(kotlin("test"))
}
