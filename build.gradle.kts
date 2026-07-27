import com.palantir.gradle.gitversion.VersionDetails

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.ktlint) apply false
    alias(libs.plugins.palantir.git.version)
    id("spring-kafka-kotlin.published-module") apply false
    // Unlike the plugins above, this one is designed to be applied exactly once at the root -- it
    // configures every subproject's apiCheck/apiDump tasks internally, rather than needing per-module
    // opt-in.
    alias(libs.plugins.binary.compatibility.validator)
}

// Only the 8 spring-kafka-kotlin-* modules are real published artifacts with a public API worth locking
// down. The shared-source modules (hook-aspect-*, micrometer-tags-provider-cache,
// send-and-receive-result-*) aren't published as their own artifacts, and the benchmark harnesses aren't
// published at all -- none of them have their own meaningful API surface to validate.
apiValidation {
    ignoredProjects.addAll(
        listOf(
            "hook-aspect-native",
            "hook-aspect-compat",
            "micrometer-tags-provider-cache",
            "send-and-receive-result-unbounded",
            "send-and-receive-result-bounded",
            "benchmarks",
            "benchmarks-compat",
        ),
    )
}

// palantir/gradle-git-version only ever registers `versionDetails` as a raw Groovy Closure via untyped
// extra properties (see GitVersionPlugin.java) -- there's no typed Kotlin extension to call instead, so
// the cast is unavoidable. Isolated here behind a small typed wrapper so callers just see a normal
// function returning VersionDetails.
// See https://github.com/palantir/gradle-git-version/issues/105
@Suppress("UNCHECKED_CAST")
fun Project.versionDetails(): VersionDetails = (extra["versionDetails"] as groovy.lang.Closure<VersionDetails>).call()

// Version is derived from the repo's git tags (see palantir/gradle-git-version), not hardcoded:
// - exactly on a tag, clean tree            -> the tag itself, e.g. "1.0.0"
// - exactly on a tag, uncommitted changes   -> "1.0.0-SNAPSHOT"
// - commits since the last tag (either way) -> "1.0.0-<shortHash>-SNAPSHOT"
// Falls back to "0.0.0-SNAPSHOT" if the repo has no commits/tags yet (git describe has nothing to
// describe) -- this is expected until the first real commit and tag are made.
val computedVersion =
    try {
        val details = versionDetails()
        val lastTag = details.lastTag.removePrefix("v")
        when {
            details.isCleanTag -> lastTag
            details.commitDistance == 0 -> "$lastTag-SNAPSHOT"
            else -> "$lastTag-${details.gitHash}-SNAPSHOT"
        }
    } catch (e: Exception) {
        logger.warn("Could not determine git version details.", e)
        "0.0.0-SNAPSHOT"
    }

allprojects {
    group = "org.mdedetrich"
    version = computedVersion

    repositories {
        mavenCentral()
    }
}

subprojects {
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    plugins.withId("org.jetbrains.kotlin.jvm") {
        dependencies {
            "testImplementation"(kotlin("test"))
        }
    }
}
