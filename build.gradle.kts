plugins {
    java
    `maven-publish`
    signing
    id("com.gradleup.shadow") version "8.3.5"
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    checkstyle
    jacoco
    id("com.diffplug.spotless") version "7.0.2"
    id("com.github.spotbugs") version "6.4.7"
}

group = "com.bbrownsound"
version = project.findProperty("version")?.toString()?.takeIf { it != "unspecified" } ?: "1.0.0-SNAPSHOT"

tasks.register("printVersion") {
    doLast { println(project.version) }
}

java {
    withSourcesJar()
    withJavadocJar()
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

val flinkVersion: String by project
val confluentVersion: String by project
val protoVersion: String by project

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    implementation("org.apache.flink:flink-core:$flinkVersion")
    implementation("org.apache.flink:flink-table-common:$flinkVersion")
    implementation("io.confluent:kafka-protobuf-serializer:$confluentVersion")
    implementation("io.confluent:kafka-protobuf-provider:$confluentVersion")
    implementation("io.confluent:kafka-schema-registry-client:$confluentVersion")
    implementation("com.google.protobuf:protobuf-java:$protoVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protoVersion")
    implementation("com.google.api.grpc:proto-google-common-protos:2.22.1")
    implementation("org.apache.commons:commons-lang3:3.14.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.2")
    testImplementation("org.apache.flink:flink-test-utils-junit:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
    testImplementation("org.testcontainers:testcontainers:2.0.3")
    testImplementation("org.testcontainers:testcontainers-kafka:2.0.3")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.3")
    testImplementation("org.awaitility:awaitility:4.2.0")
}

// Integration test: Kafka + Schema Registry + MiniCluster (optional: ClickHouse)
val integrationTest by sourceSets.creating {
    java.srcDir("src/integration-test/java")
    resources.srcDir("src/integration-test/resources")
}
configurations["integrationTestImplementation"].extendsFrom(configurations["implementation"], configurations["testImplementation"])
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["runtimeOnly"], configurations["testRuntimeOnly"])
// So integration tests can use test proto classes (e.g. SimpleMessage) and test base
integrationTest.compileClasspath += sourceSets["test"].output
integrationTest.runtimeClasspath += sourceSets["test"].output
// Ensure main output (proto-confluent format factory + META-INF/services) is on integration test classpath
integrationTest.compileClasspath += sourceSets["main"].output
integrationTest.runtimeClasspath += sourceSets["main"].output

dependencies {
    add("integrationTestImplementation", "org.apache.flink:flink-streaming-java:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-table-planner-loader:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-table-runtime:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-connector-base:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    add("integrationTestImplementation", "org.apache.flink:flink-test-utils:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-test-utils-junit:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-runtime:$flinkVersion:tests")
    add("integrationTestImplementation", "com.clickhouse:clickhouse-jdbc:0.4.6")
    add("integrationTestImplementation", "org.apache.flink:flink-connector-jdbc:3.1.0-1.17")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
    )
}
tasks.named<Test>("test").configure {
    extensions.getByType<org.gradle.testing.jacoco.plugins.JacocoTaskExtension>().apply {
        destinationFile = layout.buildDirectory.file("jacoco/test.exec").get().asFile
    }
}

tasks.shadowJar {
    archiveBaseName.set("flink-proto-confluent")
    archiveClassifier.set("")
    mergeServiceFiles()
    dependencies {
        exclude(dependency("org.apache.flink:.*:.*"))
        exclude(dependency("org.slf4j:.*:.*"))
    }
    relocate("com.google.type", "shadowed.google.type")
}

spotless {
    java {
        googleJavaFormat()
        target("src/main/java/**/*.java", "src/test/java/**/*.java")
        target("src/integration-test/java/**/*.java")
        targetExclude("src/test/java/com/bbrownsound/flink/formats/proto/test/**")
    }
}

tasks.check {
    dependsOn(
        tasks.checkstyleMain,
        tasks.checkstyleTest,
        tasks.named("checkstyleIntegrationTest"),
        tasks.spotbugsMain,
        tasks.spotbugsTest,
        tasks.spotlessCheck,
        tasks.jacocoTestCoverageVerification,
        tasks.named("integrationTest")
    )
}

checkstyle {
    toolVersion = "10.12.5"
    configFile = file("${rootDir}/config/checkstyle/checkstyle.xml")
    isIgnoreFailures = false
}

tasks.named<Checkstyle>("checkstyleTest") {
    exclude("**/com/bbrownsound/flink/formats/proto/test/**")
}

spotbugs {
    toolVersion.set("4.8.3")
    ignoreFailures.set(false)
    excludeFilter.set(file("${rootDir}/config/spotbugs/exclude.xml"))
}

jacoco {
    toolVersion = "0.8.11"
}

val integrationTestTask = tasks.register<Test>("integrationTest") {
    description = "Runs integration tests (Kafka + Schema Registry + MiniCluster; optional ClickHouse)."
    group = "verification"
    testClassesDirs = integrationTest.output.classesDirs
    classpath = integrationTest.runtimeClasspath
    shouldRunAfter(tasks.test)
    useJUnitPlatform()
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED"
    )
    testLogging {
        showStandardStreams = true
        showCauses = true
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
tasks.named<Test>("integrationTest").configure {
    extensions.getByType<org.gradle.testing.jacoco.plugins.JacocoTaskExtension>().apply {
        destinationFile = layout.buildDirectory.file("jacoco/integrationTest.exec").get().asFile
    }
}

val mainClassDirs = sourceSets.main.get().output.classesDirs.files.map { dir ->
    fileTree(dir) {
        exclude("**/CommonConstants.class")
        exclude("**/ProtoConfluentFormatOptions.class")
    }
}

tasks.jacocoTestReport {
    dependsOn(tasks.test, integrationTestTask)
    executionData.from(
        fileTree(project.file("build/jacoco")) {
            include("*.exec")
        }
    )
    classDirectories.setFrom(mainClassDirs)
    sourceSets(sourceSets.main.get())
    reports {
        xml.required.set(true)
        html.required.set(true)
        html.outputLocation.set(file("${layout.buildDirectory.get()}/reports/jacoco/test/html"))
    }
}

tasks.test {
    finalizedBy(tasks.jacocoTestReport)
}

// Sonatype Central Portal (Maven Central) via OSSRH Staging API
nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
        }
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri(
                "https://maven.pkg.github.com/${System.getenv("GITHUB_REPOSITORY") ?: "bbrownsound/flink-proto-confluent"}"
            )
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user") as String? ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.key") as String? ?: ""
            }
        }
    }
    publications {
        create<MavenPublication>("gpr") {
            from(components["java"])
            artifact(tasks.shadowJar.get()) {
                classifier = "all"
            }
            groupId = project.group.toString()
            artifactId = "flink-proto-confluent"
            version = project.version.toString()
        }
        create<MavenPublication>("sonatype") {
            from(components["java"])
            artifact(tasks.named("sourcesJar").get())
            artifact(tasks.named("javadocJar").get())
            groupId = project.group.toString()
            artifactId = "flink-proto-confluent"
            version = project.version.toString()
            pom {
                name.set(artifactId)
                description.set("Flink table format for Protobuf with Confluent Schema Registry")
                url.set("https://github.com/bbrownsound/flink-proto-confluent")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        name.set("Brandon Brown")
                        organization.set("bbrownsound")
                        organizationUrl.set("https://github.com/bbrownsound")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/bbrownsound/flink-proto-confluent-bb.git")
                    developerConnection.set("scm:git:ssh://github.com:bbrownsound/flink-proto-confluent-bb.git")
                    url.set("https://github.com/bbrownsound/flink-proto-confluent-bb")
                }
            }
        }
    }
}

// Sign Sonatype publication when credentials are present (CI or local)
val signingKey: String? by project
val signingPassword: String? by project
if (signingKey != null && signingPassword != null) {
    signing {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["sonatype"])
    }
}

// Only publish the sonatype publication to Sonatype (not gpr)
tasks.withType<PublishToMavenRepository>().configureEach {
    if (name.contains("Sonatype") && name.contains("Gpr")) {
        enabled = false
    }
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.jacocoTestReport)
    executionData.from(
        fileTree(project.file("build/jacoco")) {
            include("*.exec")
        }
    )
    classDirectories.setFrom(mainClassDirs)
    violationRules {
        rule {
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = "0.80".toBigDecimal()
            }
        }
        rule {
            element = "PACKAGE"
            includes = listOf("com.bbrownsound.flink.formats.proto.registry.confluent.serialize")
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = "0.80".toBigDecimal()
            }
        }
        rule {
            element = "PACKAGE"
            includes = listOf("com.bbrownsound.flink.formats.proto.registry.confluent.deserialize")
            limit {
                counter = "LINE"
                value = "COVEREDRATIO"
                minimum = "0.80".toBigDecimal()
            }
        }
    }
}

