import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
}

// Shared only with spring-kafka-kotlin-2.8, 2.9, 3.0, and 3.1 -- see README.md. The strictest floor
// across those 4 consumers (2.8/2.9 target JVM 8, 3.0/3.1 target JVM 17) -- code added here must stay
// compatible with the oldest consumer, since it's compiled directly into every one of them via
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
// spring-kafka-kotlin-* module (via sourceSets.main.kotlin.srcDir) -- each of those 4 modules supplies
// its own spring-kafka/spring-aop versions on its own compileKotlin classpath instead. They only matter
// for this module's own standalone compileKotlin/test, which also compiles/runs against these same files
// (they're its own main/test source sets, not just srcDir-included elsewhere) -- spring-kafka:2.8.11 (and
// its actual Spring Framework 5.3.24) is the floor across the 4 modules that share this source, and
// deliberately the version tested against here: proving the design works on the oldest, most-constrained
// case.
dependencies {
    compileOnly(libs.spring.kafka.v28)
    testImplementation(libs.spring.kafka.v28)

    compileOnly(libs.spring.aop.v28)
    compileOnly(libs.aspectjweaver)
    testImplementation(libs.spring.aop.v28)
    testImplementation(libs.aspectjweaver)

    implementation(libs.kotlinx.coroutines.jdk8)
    implementation(kotlin("reflect"))

    // JUnit 6 requires JVM 17+, incompatible with this module's JVM 8 target; latest 5.x instead.
    testImplementation(platform(libs.junit.bom.jvm8))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(kotlin("test"))
}
