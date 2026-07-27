import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-2.8 through 3.3 (not 4.0+: spring-kafka 4.0 marks
// org.springframework.kafka.core/.support/.requestreply @NullMarked (JSpecify), forcing K/V/R to be
// bound to Any -- see send-and-receive-result-bounded) -- see README.md. 2.8 is the strictest floor
// (JVM 8) across those 6 consumers -- code added here must stay compatible with it, since it's compiled
// directly into every one of them via sourceSets.main.kotlin.srcDir, not depended on as a jar.
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
// spring-kafka-kotlin-* module (via sourceSets.main.kotlin.srcDir) -- each of those 6 modules supplies
// its own spring-kafka version on its own compileKotlin classpath instead. They only matter for this
// module's own standalone compileKotlin/test, which also compiles/runs against these same files --
// spring-kafka 2.8.11 is the floor across the 6 modules that share this source, and deliberately the
// version tested against here.
dependencies {
    compileOnly(libs.spring.kafka.v28)
    testImplementation(libs.spring.kafka.v28)

    // ReplyingKafkaCoroutineOperations (the receiver type for the reified extensions here) isn't defined
    // in this module -- it's only srcDir-shared into it, same as this module's own files are. spring-kafka-
    // kotlin-2.8 is used as a representative real definition of it purely so this module's own standalone
    // compileKotlin/test can resolve the type; not a runtime dependency, and no cycle results since 2.8's
    // srcDir sharing of this module's sources doesn't add a reverse project dependency.
    compileOnly(project(":spring-kafka-kotlin-2.8"))

    implementation(libs.kotlinx.coroutines.core)

    // JUnit 6 requires JVM 17+, incompatible with this module's JVM 8 target; latest 5.x instead.
    testImplementation(platform(libs.junit.bom.jvm8))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(kotlin("test"))
}
