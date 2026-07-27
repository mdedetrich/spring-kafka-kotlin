rootProject.name = "build-logic"

// build-logic is a separate Gradle build (its own settings.gradle.kts), so the root build's
// gradle/libs.versions.toml catalog isn't auto-detected here -- wire it in explicitly so build-logic's own
// dependencies stay pinned through the same catalog instead of hardcoded versions.
dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            from(files("../gradle/libs.versions.toml"))
        }
    }
}
