plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation(libs.vanniktech.maven.publish)
    // Needed purely so the precompiled convention script can resolve the `kotlin { explicitApi() }`
    // extension type at compile time -- the consuming modules already apply this plugin themselves
    // before applying the convention plugin, so this isn't a second real application of it.
    implementation(libs.kotlin.gradle.plugin)
}
