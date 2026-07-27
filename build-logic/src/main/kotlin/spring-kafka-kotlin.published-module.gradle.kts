// Convention plugin: apply this explicitly, only in the modules that are real, publishable library
// artifacts (the spring-kafka-kotlin-* modules). Never apply it in the shared-source-only modules
// (hook-aspect-*, micrometer-tags-provider-cache, send-and-receive-result-*) or the benchmark harnesses
// (benchmarks, benchmarks-compat) -- since it's never applied there, no publication is ever registered
// and no publish task ever exists to accidentally run, no disabling required.
//
// Publishes to Sonatype's Central Portal (Maven Central) via com.vanniktech.maven.publish, which wraps
// maven-publish + signing + the Central Portal upload API. groupId/artifactId/version are left to default
// from project.group/project.name/project.version (set at the root via allprojects{} and the
// palantir-git-version-derived version) -- not overridden here via coordinates(...).
//
// Required at publish time (never hardcode these -- supply via ~/.gradle/gradle.properties or CI secrets
// as ORG_GRADLE_PROJECT_-prefixed env vars):
//   mavenCentralUsername / mavenCentralPassword  -- a user token from https://central.sonatype.com/account
//   signing.keyId / signing.password + signingInMemoryKey (or signing.secretKeyRingFile for a local key)
//     -- a GPG key registered to a public keyserver; see
//     https://vanniktech.github.io/gradle-maven-publish-plugin/central/#signing-with-a-registered-gpg-key
//
// publishToMavenCentral() deliberately does NOT enable automaticRelease -- a staged deployment lands in
// the Central Portal awaiting manual review/release, since releasing to Central is effectively
// irreversible (immutable coordinates) and shouldn't happen automatically as a side effect of running
// `publish`.
plugins {
    id("com.vanniktech.maven.publish")
    // Applying this a second time (every consuming module already applies it itself before applying
    // this convention plugin) is a harmless no-op at the project level -- needed here only so Gradle
    // generates the `kotlin { }` type-safe accessor for this precompiled script. Unlike
    // com.vanniktech.maven.publish, this plugin doesn't register a cross-project shared BuildService, so
    // it doesn't hit the classloader-scope issue described above.
    id("org.jetbrains.kotlin.jvm")
}

// Every public declaration must have an explicit visibility modifier and return type -- catches
// accidentally-public API surface (a missing `internal`/`private`, an inferred return type that's wider
// than intended) before it ships in a release. Applies here (not in the shared-source modules' own
// standalone builds) since this is where their srcDir-included files actually become part of a real
// published artifact's public API.
kotlin {
    explicitApi()
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    pom {
        name.set(project.name)
        description.set("Kotlin coroutines wrapper for spring-kafka (${project.name}).")
        url.set("https://github.com/mdedetrich/spring-kafka-kotlin")

        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }

        developers {
            developer {
                id.set("mdedetrich")
                name.set("mdedetrich")
            }
        }

        scm {
            url.set("https://github.com/mdedetrich/spring-kafka-kotlin")
            connection.set("scm:git:https://github.com/mdedetrich/spring-kafka-kotlin.git")
            developerConnection.set("scm:git:ssh://git@github.com/mdedetrich/spring-kafka-kotlin.git")
        }
    }
}
