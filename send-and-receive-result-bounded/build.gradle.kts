import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.ktlint)
}

// Shared with spring-kafka-kotlin-4.0 and 4.1 (not pre-4.0: spring-kafka 4.0 marks
// org.springframework.kafka.core/.support/.requestreply @NullMarked (JSpecify), forcing K/V/R to be
// bound to Any here -- see send-and-receive-result-unbounded for the pre-4.0 variant) -- see README.md.
// Both consumers already target JVM 17, so no lower floor to accommodate here.
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
// spring-kafka-kotlin-* module (via sourceSets.main.kotlin.srcDir) -- each of those 2 modules supplies
// its own spring-kafka version on its own compileKotlin classpath instead. They only matter for this
// module's own standalone compileKotlin/test, which also compiles/runs against these same files --
// spring-kafka 4.0.6 is the floor across the 2 modules that share this source, and deliberately the
// version tested against here.
dependencies {
    compileOnly(libs.spring.kafka.v40)
    testImplementation(libs.spring.kafka.v40)

    // ReplyingKafkaCoroutineOperations (the receiver type for the reified extensions here) isn't defined
    // in this module -- it's only srcDir-shared into it, same as this module's own files are. spring-kafka-
    // kotlin-4.0 is used as a representative real definition of it purely so this module's own standalone
    // compileKotlin/test can resolve the type; not a runtime dependency, and no cycle results since 4.0's
    // srcDir sharing of this module's sources doesn't add a reverse project dependency.
    compileOnly(project(":spring-kafka-kotlin-4.0"))

    implementation(libs.kotlinx.coroutines.core)

    // JUnit 6 requires JVM 17+; safe here since this module already targets JVM 17.
    testImplementation(platform(libs.junit.bom.jvm17))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation(kotlin("test"))
}
