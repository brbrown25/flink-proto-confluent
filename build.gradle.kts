import java.util.jar.Manifest as JarManifest
import java.util.zip.ZipFile
import me.champeau.gradle.japicmp.JapicmpTask

plugins {
    java
    `maven-publish`
    signing
    id("com.gradleup.shadow") version "9.6.1"
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    checkstyle
    jacoco
    id("com.diffplug.spotless") version "8.11.0"
    id("com.github.spotbugs") version "6.5.13"
    id("me.champeau.jmh") version "0.7.4"
    id("me.champeau.gradle.japicmp") version "0.4.6"
}

group = "com.bbrownsound"

/**
 * Derives the next patch version from the last released one, e.g. "1.0.0" -> "1.0.1". Used only
 * to build a local snapshot version; CI always supplies -Pversion= explicitly.
 */
fun nextPatchAfter(released: String): String {
    val parts = released.trim().removePrefix("v").split(".")
    require(parts.size == 3 && parts.all { it.toIntOrNull() != null }) {
        "releaseVersion in gradle.properties must be MAJOR.MINOR.PATCH, got '$released'"
    }
    return "${parts[0]}.${parts[1]}.${parts[2].toInt() + 1}"
}

// `releaseVersion` in gradle.properties is the last version published to Maven Central, owned by
// release-please. Both CI publish paths pass an explicit -Pversion= that wins over it: release.yml
// passes the tag version, publish-snapshot.yml passes a SHA-qualified snapshot. A build with no
// override (any local build) gets the next patch as a -SNAPSHOT, so it can never collide with a
// coordinate that has already been published.
val releaseVersion: String by project
version = project.findProperty("version")?.toString()?.takeIf { it != "unspecified" }
    ?: "${nextPatchAfter(releaseVersion)}-SNAPSHOT"

tasks.register("printVersion") {
    val resolved = project.version.toString()
    doLast { println(resolved) }
}

java {
    withSourcesJar()
    withJavadocJar()
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

// -PflinkVersion=<v> picks the Flink version; without it the build uses the Flink 1 line's version,
// flink1.version in gradle.properties, which is also what CI tests and releases ship (#249).
val flinkVersion: String =
    (findProperty("flinkVersion") ?: findProperty("flink1.version"))?.toString()
        ?: throw GradleException("gradle.properties has no 'flink1.version'.")
val confluentVersion: String by project
val protoVersion: String by project

// The Flink line is the major version of flinkVersion. Coordinates that are released per Flink
// line (connectors) are read from `flink<line>.*` properties in gradle.properties, so a build
// never mixes artifacts from two lines.
val supportedFlinkLines = listOf("1", "2")
val flinkLine: String = flinkVersion.trim().substringBefore('.').let { major ->
    if (major !in supportedFlinkLines) {
        throw GradleException(
            "Unsupported flinkVersion '$flinkVersion': its major version '$major' is not a " +
                "supported Flink line. Supported lines: ${supportedFlinkLines.joinToString()} " +
                "(e.g. -PflinkVersion=1.20.5 or -PflinkVersion=2.2.1)."
        )
    }
    major
}

fun flinkLineProperty(name: String): String {
    val key = "flink$flinkLine.$name"
    return project.findProperty(key)?.toString()
        ?: throw GradleException(
            "gradle.properties has no '$key' for Flink line $flinkLine (flinkVersion=$flinkVersion)."
        )
}

val kafkaConnectorVersion = flinkLineProperty("kafkaConnectorVersion")

// Each Flink line publishes under its own artifactId (same group, same version). Every JAR the
// build writes (thin, -all, -sources, -javadoc) is named after it, so the two lines' JARs can sit
// side by side on a GitHub Release.
val publishedArtifactId = flinkLineProperty("artifactId")
base {
    archivesName.set(publishedArtifactId)
}

tasks.register("printArtifactId") {
    val id = publishedArtifactId
    doLast { println(id) }
}

repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

dependencies {
    // Flink is provided by the cluster (or by the consuming job), so it must not leak into the
    // published POM or the shadow JAR. Tests get it back through testImplementation below.
    compileOnly("org.apache.flink:flink-core:$flinkVersion")
    compileOnly("org.apache.flink:flink-table-common:$flinkVersion")
    implementation("io.confluent:kafka-protobuf-serializer:$confluentVersion")
    implementation("io.confluent:kafka-protobuf-provider:$confluentVersion")
    implementation("io.confluent:kafka-schema-registry-client:$confluentVersion")
    implementation("com.google.protobuf:protobuf-java:$protoVersion")
    implementation("com.google.protobuf:protobuf-java-util:$protoVersion")
    implementation("com.google.api.grpc:proto-google-common-protos:2.81.0")
    implementation("org.apache.commons:commons-lang3:3.22.0")

    testImplementation("org.junit.jupiter:junit-jupiter:6.1.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:6.1.3")
    testImplementation("org.apache.flink:flink-test-utils-junit:$flinkVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion:tests")
    testImplementation("org.apache.flink:flink-core:$flinkVersion")
    testImplementation("org.apache.flink:flink-table-common:$flinkVersion")
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:testcontainers-kafka:2.0.5")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.5")
    testImplementation("org.awaitility:awaitility:4.3.0")
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
    add("integrationTestImplementation", "org.apache.flink:flink-connector-datagen:$flinkVersion")
    // The filesystem connector pushes projections into its format (ProjectionPushdown IT, #173).
    add("integrationTestImplementation", "org.apache.flink:flink-connector-files:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-connector-kafka:$kafkaConnectorVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-test-utils:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-test-utils-junit:$flinkVersion")
    add("integrationTestImplementation", "org.apache.flink:flink-runtime:$flinkVersion:tests")
    add("integrationTestImplementation", "com.clickhouse:clickhouse-jdbc:0.11.2")
}

// clickhouse-jdbc 0.10.0 depends on the at.yawk.lz4:lz4-java fork, which declares the same
// org.lz4:lz4-java capability as the lz4-java that flink-runtime pulls in. The fork is a
// drop-in replacement (same net.jpountz packages), so prefer it whenever both are present.
configurations.all {
    resolutionStrategy.capabilitiesResolution.withCapability("org.lz4", "lz4-java") {
        val fork = candidates.firstOrNull {
            val id = it.id
            id is ModuleComponentIdentifier && id.group == "at.yawk.lz4"
        }
        if (fork != null) {
            select(fork)
            because("at.yawk.lz4:lz4-java is a drop-in fork required by clickhouse-jdbc")
        }
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
    )
}

// FlinkLineGuardTest and FlinkLineGuardIntegrationTest fail a test run whose classpath does not
// match the Flink line it claims to test, so a CI leg can never pass by quietly testing the
// default line. CI also sets EXPECTED_FLINK_VERSION from its matrix, independently of Gradle, which
// catches a -PflinkVersion that Gradle ignored. Passed as system properties so they are task inputs.
val ciExpectedFlinkVersion = providers.environmentVariable("EXPECTED_FLINK_VERSION").orElse("").get()
tasks.withType<Test>().configureEach {
    systemProperty("flinkLine.flinkVersion", flinkVersion)
    systemProperty("flinkLine.kafkaConnectorVersion", kafkaConnectorVersion)
    systemProperty("flinkLine.ciExpectedFlinkVersion", ciExpectedFlinkVersion)
}

// OptionsDocumentationTest checks the option tables of these pages against the ConfigOptions. It
// reads them relative to the test working directory (the project directory), and they are inputs
// of the unit tests, so editing a table reruns them.
val optionsDocsDir = layout.projectDirectory.dir("docs/content")
tasks.named<Test>("test").configure {
    inputs.files(optionsDocsDir.file("reference/configuration.md"), optionsDocsDir.file("how-to/catalog.md"))
        .withPropertyName("optionsDocs")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    extensions.getByType<org.gradle.testing.jacoco.plugins.JacocoTaskExtension>().apply {
        destinationFile = layout.buildDirectory.file("jacoco/test.exec").get().asFile
    }
}

// The shadow JAR doubles as the `ddl` command line tool (cli.Main). Flink provides SLF4J, so the
// shadow JAR leaves it out, but the Schema Registry client cannot load without the SLF4J API. A
// copy of slf4j-api rides along as a nested JAR under a name no class loader looks at (so it is
// invisible in Flink's lib/); cli.Main puts it on a private class loader only when `java -jar`
// runs the tool without SLF4J on the classpath. Its version follows the one the format resolves.
val cliEmbeddedSlf4j by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
    isTransitive = false
    shouldResolveConsistentlyWith(configurations.runtimeClasspath.get())
}
dependencies {
    cliEmbeddedSlf4j("org.slf4j:slf4j-api")
}

tasks.shadowJar {
    archiveBaseName.set(publishedArtifactId)
    archiveClassifier.set("all")
    mergeServiceFiles()
    manifest {
        attributes("Main-Class" to "com.bbrownsound.flink.formats.proto.registry.confluent.cli.Main")
    }
    from(cliEmbeddedSlf4j) {
        into("META-INF/proto-confluent-cli")
        rename { "slf4j-api.jar.bin" }
    }
    dependencies {
        exclude(dependency("org.apache.flink:.*:.*"))
        exclude(dependency("org.slf4j:.*:.*"))
    }
    relocate("com.google.type", "shadowed.google.type")
    // Libraries that commonly sit on a Flink classpath at other versions are moved out of the way.
    // Deliberately NOT relocated (see docs/content/reference/classpath.md):
    //  - com.google.protobuf: user-generated message classes (message-class) link against it.
    //  - io.confluent, org.apache.kafka: users name these classes in option strings (subject name
    //    strategies, sasl.jaas.config login modules), which a relocation would silently break.
    //  - JNI-backed codecs (zstd-jni, snappy-java, lz4-java): native symbols are bound to the
    //    original package names.
    //  - kotlin, okio, com.squareup.wire: Kotlin metadata and kotlin-reflect do not survive it.
    listOf(
        "com.fasterxml.jackson",
        "com.google.common",
        "com.google.thirdparty",
        "com.google.gson",
        "org.apache.commons",
        "org.apache.hc",
        "org.apache.avro",
        "org.yaml.snakeyaml",
        "io.swagger",
    ).forEach { relocate(it, "com.bbrownsound.flink.formats.proto.shaded.$it") }
}

// Guards the packaging contract: the unclassified JAR is the thin library JAR and the "all" JAR is
// self-contained with the relocations above applied. Both used to be written to the same file name,
// so the shadow JAR silently replaced the thin one in every published artifact.
val verifyJars = tasks.register("verifyJars") {
    description = "Checks the contents of the thin JAR and the shadow (all) JAR."
    group = "verification"
    val thinJar = tasks.jar.flatMap { it.archiveFile }
    val allJar = tasks.shadowJar.flatMap { it.archiveFile }
    inputs.file(thinJar)
    inputs.file(allJar)
    doLast {
        fun classes(jar: File) = ZipFile(jar).use { zip ->
            zip.entries().asSequence().map { it.name }.filter { it.endsWith(".class") }.toList()
        }
        val own = "com/bbrownsound/"
        val foreign = classes(thinJar.get().asFile).filterNot { it.startsWith(own) }
        check(foreign.isEmpty()) {
            "Thin JAR must contain only this project's classes, found e.g. ${foreign.take(5)}"
        }
        val all = classes(allJar.get().asFile)
        val mustBeAbsent = listOf(
            "org/apache/flink/", "org/slf4j/",
            "com/fasterxml/jackson/", "com/google/common/", "com/google/thirdparty/",
            "com/google/gson/", "org/apache/commons/", "org/apache/hc/", "org/apache/avro/",
            "org/yaml/snakeyaml/", "io/swagger/",
        )
        val leaked = all.filter { name -> mustBeAbsent.any { name.startsWith(it) } }
        check(leaked.isEmpty()) {
            "Shadow JAR has provided or unrelocated classes, e.g. ${leaked.take(5)}"
        }
        val mustBePresent = listOf(
            own, "com/google/protobuf/", "io/confluent/kafka/", "org/apache/kafka/",
            "com/bbrownsound/flink/formats/proto/shaded/com/fasterxml/jackson/",
        )
        val missing = mustBePresent.filter { prefix -> all.none { it.startsWith(prefix) } }
        check(missing.isEmpty()) { "Shadow JAR is missing expected packages: $missing" }
        // The `ddl` command line tool: an entry point, and the SLF4J API it needs outside Flink.
        ZipFile(allJar.get().asFile).use { zip ->
            val mainClass = JarManifest(
                zip.getInputStream(zip.getEntry("META-INF/MANIFEST.MF"))
            ).mainAttributes.getValue("Main-Class")
            check(mainClass == "com.bbrownsound.flink.formats.proto.registry.confluent.cli.Main") {
                "Shadow JAR manifest must name the ddl tool as Main-Class, found '$mainClass'"
            }
            check(zip.getEntry("META-INF/proto-confluent-cli/slf4j-api.jar.bin") != null) {
                "Shadow JAR is missing the embedded slf4j-api the ddl tool needs"
            }
        }
    }
}

// Guards the published protobuf version (issue #189). `protoVersion` is what the POM advertises and
// what the classpath reference tells users to pin. If a transitive dependency asks for a newer
// protobuf-java, Gradle quietly resolves the newer one here, the tests pass against it, and the
// POM keeps naming a version the library no longer works with.
val protobufModules = listOf("protobuf-java", "protobuf-java-util")
val verifyProtobufVersion = tasks.register("verifyProtobufVersion") {
    description = "Checks that protoVersion is the protobuf version runtimeClasspath resolves."
    group = "verification"
    val declared = protoVersion
    val modules = protobufModules
    val resolved = configurations.runtimeClasspath.flatMap { classpath ->
        classpath.incoming.artifacts.resolvedArtifacts.map { artifacts ->
            artifacts.mapNotNull { it.id.componentIdentifier as? ModuleComponentIdentifier }
                .filter { it.group == "com.google.protobuf" && it.module in modules }
                .associate { it.module to it.version }
        }
    }
    inputs.property("declared", declared)
    inputs.property("resolved", resolved)
    doLast {
        val versions = resolved.get()
        val absent = modules.filterNot { it in versions }
        check(absent.isEmpty()) { "runtimeClasspath has no com.google.protobuf:$absent" }
        val drifted = versions.filterValues { it != declared }
        check(drifted.isEmpty()) {
            "gradle.properties declares protoVersion=$declared, but runtimeClasspath resolves " +
                drifted.map { (module, version) -> "$module:$version" }.joinToString() +
                ". The POM would advertise a protobuf version the library is not built and " +
                "tested with. Raise protoVersion to the resolved version (the highest one if " +
                "they differ) and update the pin examples in " +
                "docs/content/reference/classpath.md. To see what pulls the version up: " +
                "./gradlew dependencyInsight --dependency protobuf-java " +
                "--configuration runtimeClasspath"
        }
    }
}

// The test classpath with protobuf pinned to exactly `protoVersion`, the way the classpath
// reference tells a job build to pin it. `declaredProtobufTest` runs the format on it, so a
// declared version that the format's dependencies cannot initialise against fails here with the
// error a user would get, rather than only as a version mismatch in verifyProtobufVersion.
val declaredProtobufRuntimeClasspath by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
    extendsFrom(configurations.testImplementation.get(), configurations.testRuntimeOnly.get())
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
        attribute(
            LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
            objects.named(LibraryElements.JAR)
        )
    }
    resolutionStrategy.force(protobufModules.map { "com.google.protobuf:$it:$protoVersion" })
}

val declaredProtobufTest = tasks.register<Test>("declaredProtobufTest") {
    description = "Runs the format against exactly the declared protobuf-java version."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.main.get().output + sourceSets.test.get().output +
        declaredProtobufRuntimeClasspath
    useJUnitPlatform()
    filter { includeTestsMatching("*DeclaredProtobufRuntimeTest") }
    systemProperty("declaredProtoVersion", protoVersion)
    jvmArgs(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
    )
    // Same classes as `test`, on a different classpath: it must not add a second set of
    // execution data to the coverage report.
    extensions.configure<org.gradle.testing.jacoco.plugins.JacocoTaskExtension> {
        isEnabled = false
    }
}

// API compatibility check (issue #220). `japicmp` compares the thin JAR of the current Flink line
// with the last release of that line's artifact on Maven Central (`releaseVersion`, owned by
// release-please) and fails on any binary or source incompatible change to the public API. The
// shadow (-all) JAR is never compared: the relocated packages in it are not API. CI runs it on
// every pull request for both Flink lines and lets it pass only when the pull request declares the
// break (`!` after the type/scope, or a `BREAKING CHANGE:` footer), which makes release-please bump
// the major version; locally, -Pjapicmp.acceptBreaking=true does the same. It is not part of
// `check`, because it needs Maven Central. See docs/ci.md and CONTRIBUTING.md.
//
// The public API is listed here, and only here. Everything else in the JAR (the decoding and
// encoding formats the factory hands to Flink, the converters, the (de)serialization schemas, the
// metrics, the `ddl` command line tool and the rest of `util`) is internal and may change in any
// release. A class or package added to the list below becomes subject to the check.
val apiBasePackage = "com.bbrownsound.flink.formats.proto.registry.confluent"
val publicApiPackages = listOf(
    // ProtoConfluentFormatConfig and the enums its options map to.
    "$apiBasePackage.config",
    // SchemaRegistryCatalog, its factory, options and config.
    "$apiBasePackage.catalog",
    // SubjectNameStrategies and FixedSubjectNameStrategy, which users name in option strings.
    "$apiBasePackage.subject",
    // SchemaRegistrySchemaResolver, ResolvedProtoSchema, SchemaRegistryClients: documented Java API
    // for deriving a schema outside a Flink job (docs/content/how-to/generate-ddl.md).
    "$apiBasePackage.schema",
)
val publicApiClasses = listOf(
    "$apiBasePackage.RegistryProtoFormatFactory",
    "$apiBasePackage.ProtoConfluentFormatOptions",
    // Surfaces to users as the cause of a deserialize error.
    "$apiBasePackage.deserialize.UnexpectedMessageTypeException",
    // Documented Java API for deriving DDL and Flink types (generate-ddl.md, type-mapping.md).
    "$apiBasePackage.util.ProtoToTableDDL",
    "$apiBasePackage.util.ProtoToLogicalType",
    "$apiBasePackage.util.ProtoToSqlType",
    "$apiBasePackage.util.RecursionStrategy",
).flatMap { listOf(it, "$it\$*") }

// Intentional breaks, keyed by the baseline version they break. An entry is a japicmp method,
// field or class exclusion, with a comment naming the issue or pull request that made the break.
// Entries for a baseline stop applying once release-please moves `releaseVersion` past it, so the
// list never hides a later change to the same member. Record a break here only together with a
// breaking-change marker on the pull request (see CONTRIBUTING.md).
data class AcceptedBreaks(
    val methods: List<String> = emptyList(),
    val fields: List<String> = emptyList(),
    val classes: List<String> = emptyList(),
)
val acceptedBreaks: Map<String, AcceptedBreaks> = mapOf(
    // None: nothing on main since 1.2.0 changes the public API incompatibly (checked for #220).
    // Shape of an entry, as the japicmp report prints it:
    //   methods = listOf(
    //       // #123: printTableSchemaFromProto removed, use tableSchemaFromProto.
    //       "com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToTableDDL" +
    //           "#printTableSchemaFromProto(java.lang.Class)",
    //   ),
    "1.2.0" to AcceptedBreaks(),
)
val acceptedForBaseline = acceptedBreaks[releaseVersion] ?: AcceptedBreaks()

val japicmpBaselineCoordinates = "${project.group}:$publishedArtifactId:$releaseVersion"
// Detached, so the baseline's own coordinates never resolve to this project, and non-transitive:
// only the JAR is compared. Resolution is lenient, so a line with no release yet (the artifact is
// absent from Central) skips the check instead of failing it.
val japicmpBaseline = configurations.detachedConfiguration(
    dependencies.create("$japicmpBaselineCoordinates@jar")
).apply { isTransitive = false }
val japicmpBaselineArtifacts = japicmpBaseline.incoming.artifactView { isLenient = true }.artifacts

/**
 * One element of the public API with incompatible changes: the element, the japicmp change types,
 * and the acceptedBreaks entry that would exclude it.
 */
data class ApiBreak(
    val element: String,
    val changes: List<String>,
    val binaryCompatible: Boolean,
    val sourceCompatible: Boolean,
    val exclusion: String,
)

/**
 * Reads the incompatible changes out of japicmp's XML report. The XML is the source of truth
 * because japicmp's own "only incompatible" filter and failure flag see binary changes only, and
 * miss changes that are only source incompatible, such as a method that now throws a checked
 * exception.
 */
fun readApiBreaks(xml: File): List<ApiBreak> {
    val doc = javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xml)
    fun org.w3c.dom.Node.ancestor(vararg tags: String): org.w3c.dom.Element? =
        generateSequence(parentNode) { it.parentNode }
            .filterIsInstance<org.w3c.dom.Element>()
            .firstOrNull { it.tagName in tags }
    fun org.w3c.dom.Element.parameterTypes(): String {
        val parameters = getElementsByTagName("parameters").item(0) as? org.w3c.dom.Element
            ?: return ""
        val list = parameters.getElementsByTagName("parameter")
        return (0 until list.length).joinToString(",") {
            (list.item(it) as org.w3c.dom.Element).getAttribute("type")
        }
    }
    val changes = doc.getElementsByTagName("compatibilityChange")
    val byElement = linkedMapOf<String, ApiBreak>()
    for (i in 0 until changes.length) {
        val change = changes.item(i) as org.w3c.dom.Element
        val binary = change.getAttribute("binaryCompatible").toBoolean()
        val source = change.getAttribute("sourceCompatible").toBoolean()
        if (binary && source) continue
        val owner = change.ancestor("method", "constructor", "field", "class") ?: continue
        val classElement = (if (owner.tagName == "class") owner else owner.ancestor("class"))
            ?: continue
        // A class that is new since the baseline cannot break a caller; japicmp still reports,
        // for example, the abstract methods of the interfaces it implements as "added".
        if (classElement.getAttribute("changeStatus") == "NEW") continue
        val cls = classElement.getAttribute("fullyQualifiedName")
        val (list, element) = when (owner.tagName) {
            "method", "constructor" ->
                "methods" to "$cls#${owner.getAttribute("name")}(${owner.parameterTypes()})"
            "field" -> "fields" to "$cls#${owner.getAttribute("name")}"
            else -> "classes" to cls
        }
        // Written as a Kotlin string literal, ready to paste into acceptedBreaks.
        val exclusion = "$list: \"${element.replace("$", "\\$")}\""
        val previous = byElement[element]
        byElement[element] = ApiBreak(
            element,
            ((previous?.changes ?: emptyList()) + change.getAttribute("type")).distinct(),
            (previous?.binaryCompatible ?: true) && binary,
            (previous?.sourceCompatible ?: true) && source,
            exclusion,
        )
    }
    return byElement.values.toList()
}

tasks.register<JapicmpTask>("japicmp") {
    description = "Checks the public API for incompatible changes against the last release."
    group = "verification"
    val baseline = japicmpBaselineArtifacts
    val coordinates = japicmpBaselineCoordinates
    val baselineVersion = releaseVersion
    oldArchives.from(baseline.artifactFiles)
    oldClasspath.from(configurations.compileClasspath)
    newArchives.from(tasks.jar.flatMap { it.archiveFile })
    newClasspath.from(configurations.compileClasspath)
    packageIncludes.set(publicApiPackages)
    classIncludes.set(publicApiClasses)
    methodExcludes.set(acceptedForBaseline.methods)
    fieldExcludes.set(acceptedForBaseline.fields)
    classExcludes.set(acceptedForBaseline.classes)
    accessModifier.set("protected")
    onlyModified.set(true)
    // japicmp's own failure flags and "only incompatible" filter see binary changes only. The
    // action below fails on binary and source incompatible changes alike, read from the XML report.
    onlyBinaryIncompatibleModified.set(false)
    failOnModification.set(false)
    failOnSourceIncompatibility.set(false)
    val acceptBreaking = providers.gradleProperty("japicmp.acceptBreaking")
        .map { it.toBoolean() }.orElse(false)
    inputs.property("acceptBreaking", acceptBreaking)
    val reports = layout.buildDirectory.dir("reports/japicmp")
    // Every changed element of the public API, each marked binary/source compatible or not.
    htmlOutputFile.set(reports.map { it.file("japicmp.html") })
    xmlOutputFile.set(reports.map { it.file("japicmp.xml") })
    // Only the incompatible changes. CI posts it as the job summary.
    val markdown = reports.map { it.file("japicmp.md") }
    outputs.file(markdown)
    onlyIf("the baseline $coordinates is on Maven Central") {
        val failures = baseline.failures
        if (failures.isEmpty()) {
            return@onlyIf true
        }
        // Only "this version does not exist" means there is no baseline. Anything else (no network,
        // a 5xx after Gradle's retries) fails, so CI never passes a check it did not run.
        val notFound = failures.all { failure ->
            generateSequence(failure) { it.cause }
                .any { it.javaClass.simpleName == "ModuleVersionNotFoundException" }
        }
        if (!notFound) {
            throw GradleException(
                "japicmp: could not resolve the baseline $coordinates. Check the network or " +
                    "the Maven Central status and retry.",
                failures.first()
            )
        }
        val reason = "$coordinates is not on Maven Central, so this Flink line has no released " +
            "API to compare with yet. The check starts with the line's first release."
        logger.lifecycle("japicmp: SKIPPED. $reason")
        // Replaces any earlier report, which would describe a different build.
        markdown.get().asFile.apply { parentFile.mkdirs() }.writeText(
            "## Public API compatibility with $coordinates\n\nSkipped: $reason\n"
        )
        false
    }
    doLast {
        val breaks = readApiBreaks(xmlOutputFile.get().asFile)
        fun verdict(compatible: Boolean) = if (compatible) "compatible" else "**incompatible**"
        val report = buildString {
            appendLine("## Public API compatibility with $coordinates")
            appendLine()
            if (breaks.isEmpty()) {
                appendLine("No binary or source incompatible changes to the public API.")
                return@buildString
            }
            appendLine(
                "${breaks.size} element(s) of the public API changed incompatibly. Merging needs " +
                    "a breaking-change marker on the pull request, so that the next release is a " +
                    "major version (see CONTRIBUTING.md)."
            )
            appendLine()
            appendLine("| Element | Change | Binary | Source |")
            appendLine("| --- | --- | --- | --- |")
            breaks.forEach {
                appendLine(
                    "| `${it.element}` | ${it.changes.joinToString(", ")} | " +
                        "${verdict(it.binaryCompatible)} | ${verdict(it.sourceCompatible)} |"
                )
            }
            appendLine()
            appendLine(
                "To accept an intentional break, add its entry to " +
                    "`acceptedBreaks[\"$baselineVersion\"]` in `build.gradle.kts`, with a comment " +
                    "naming the issue:"
            )
            appendLine()
            appendLine("```")
            breaks.forEach { appendLine(it.exclusion) }
            appendLine("```")
        }
        markdown.get().asFile.writeText(report)
        if (breaks.isEmpty()) {
            logger.lifecycle("japicmp: no incompatible changes to the public API since $coordinates.")
            return@doLast
        }
        val list = breaks.joinToString("\n") { "  - ${it.element}: ${it.changes.joinToString()}" }
        if (acceptBreaking.get()) {
            logger.warn(
                "japicmp: ${breaks.size} incompatible change(s) since $coordinates, accepted " +
                    "because japicmp.acceptBreaking=true:\n$list"
            )
        } else {
            throw GradleException(
                "japicmp: ${breaks.size} incompatible change(s) to the public API since " +
                    "$coordinates:\n$list\nSee ${markdown.get().asFile}. Restore compatibility, " +
                    "or declare the break (see CONTRIBUTING.md) and run with " +
                    "-Pjapicmp.acceptBreaking=true."
            )
        }
    }
}

// JMH benchmarks (src/jmh): the per-record deserialize and serialize paths over the test messages,
// against an in-memory Schema Registry. `./gradlew jmh` runs them and writes JSON results to
// build/results/jmh/results.json. They are not part of `check`, `test` or coverage; the source set
// is only compiled and linted there. Every run setting below can be overridden with a project
// property, e.g. `./gradlew jmh -Pjmh.fork=2 -Pjmh.iterations=10 -Pjmh.includes=Deserialize`.
// See docs/content/reference/benchmarks.md.
fun jmhProperty(name: String): String? =
    project.findProperty("jmh.$name")?.toString()?.takeIf { it.isNotBlank() }

jmh {
    // Benchmarks reuse the generated test messages (TestSimple, TestBig, ...) and Flink, which the
    // main source set only has at compile time.
    includeTests = true
    jmhVersion = "1.37"
    fork = jmhProperty("fork")?.toInt() ?: 1
    warmupIterations = jmhProperty("warmupIterations")?.toInt() ?: 3
    warmup = jmhProperty("warmup") ?: "2s"
    iterations = jmhProperty("iterations")?.toInt() ?: 5
    timeOnIteration = jmhProperty("timeOnIteration") ?: "2s"
    jmhProperty("includes")?.let { includes = it.split(",") }
    jmhProperty("profilers")?.let { profilers = it.split(",") }
    resultFormat = "JSON"
    resultsFile = layout.buildDirectory.file("results/jmh/results.json")
    humanOutputFile = layout.buildDirectory.file("results/jmh/human.txt")
    jvmArgsAppend = listOf(
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
}

dependencies {
    add("jmhImplementation", "org.apache.flink:flink-core:$flinkVersion")
    add("jmhImplementation", "org.apache.flink:flink-table-common:$flinkVersion")
}

spotless {
    java {
        googleJavaFormat()
        target("src/main/java/**/*.java", "src/test/java/**/*.java")
        target("src/integration-test/java/**/*.java")
        target("src/jmh/java/**/*.java")
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
        verifyJars,
        verifyProtobufVersion,
        declaredProtobufTest,
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
    // DdlCliIntegrationTest also runs `java -jar <shadow JAR> ddl ...` as a separate process.
    val shadowJarFile = tasks.shadowJar.flatMap { it.archiveFile }
    inputs.file(shadowJarFile)
    val testClassesDirs = sourceSets.test.get().output.classesDirs
    jvmArgumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "-Dcli.shadowJar=${shadowJarFile.get().asFile.absolutePath}",
            "-Dcli.testClasses=${testClassesDirs.files.joinToString(File.pathSeparator)}",
        )
    })
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
    // Names the staging repository after the coordinates it holds. Each Gradle invocation creates
    // and then closes/releases only the staging repository it started itself, so the release
    // workflow publishes the lines one invocation at a time (see .github/workflows/release.yml).
    repositoryDescription.set("${project.group}:$publishedArtifactId:${project.version}")
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
                "https://maven.pkg.github.com/${System.getenv("GITHUB_REPOSITORY") ?: "brbrown25/flink-proto-confluent"}"
            )
            credentials {
                username = System.getenv("GITHUB_ACTOR") ?: project.findProperty("gpr.user") as String? ?: ""
                password = System.getenv("GITHUB_TOKEN") ?: project.findProperty("gpr.key") as String? ?: ""
            }
        }
    }
    publications {
        create<MavenPublication>("gpr") {
            // components["java"] already carries the shadow JAR (classifier "all") as the
            // shadowRuntimeElements variant, so it is not attached again here.
            from(components["java"])
            groupId = project.group.toString()
            artifactId = publishedArtifactId
            version = project.version.toString()
        }
        create<MavenPublication>("sonatype") {
            from(components["java"])
            groupId = project.group.toString()
            artifactId = publishedArtifactId
            version = project.version.toString()
            pom {
                name.set(artifactId)
                description.set(
                    "Flink table format for Protobuf with Confluent Schema Registry " +
                        "(Flink $flinkLine.x line, built against Flink $flinkVersion)"
                )
                url.set("https://github.com/brbrown25/flink-proto-confluent")
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
                        organizationUrl.set("https://github.com/brbrown25")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/brbrown25/flink-proto-confluent.git")
                    developerConnection.set("scm:git:ssh://github.com:brbrown25/flink-proto-confluent.git")
                    url.set("https://github.com/brbrown25/flink-proto-confluent")
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

