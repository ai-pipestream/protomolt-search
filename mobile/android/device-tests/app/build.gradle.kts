import com.google.protobuf.gradle.*

plugins {
    id("com.android.application")
    id("com.google.protobuf")
}

val checkout = rootProject.projectDir.resolve("../../..").canonicalFile
val sdkAar = providers.gradleProperty("protomoltAar")
    .map { rootProject.file(it) }
    .getOrElse(checkout.resolve("target/mobile/ProtomoltSearch.aar"))

android {
    namespace = "ai.pipestream.search.mobile.devicetest"
    compileSdk = 37

    defaultConfig {
        applicationId = "ai.pipestream.search.mobile.devicetest"
        minSdk = 26
        targetSdk = 37
        versionCode = 1
        versionName = "1"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    sourceSets.getByName("main").proto {
        srcDir(checkout.resolve("proto"))
        include("ai/pipestream/search/v1/*.proto")
        include("ai/pipestream/search/mobile/v1/mobile.proto")
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.36.1"
    }
}

dependencies {
    implementation(files(sdkAar))
    implementation("com.google.protobuf:protobuf-java:4.36.1")

    androidTestImplementation("androidx.test:core:1.7.0")
    androidTestImplementation("androidx.test:runner:1.7.0")
    androidTestImplementation("androidx.test.ext:junit:1.3.0")
}
