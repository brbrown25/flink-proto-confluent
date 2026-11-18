# Classpath and dependency conflicts

The format is published as two JARs per Flink line (see [Supported Flink versions](../getting-started.md#supported-flink-versions)). Which one you use decides who is responsible for the dependencies.

| Artifact | Use it for | Dependencies |
| --- | --- | --- |
| `flink-proto-confluent-<version>.jar` (Flink 1.20), `flink-proto-confluent-flink2-<version>.jar` (Flink 2.x) | A job you build with Gradle, Maven or sbt | Resolved by your build from the POM. Needs the Confluent Maven repository (`https://packages.confluent.io/maven/`). |
| `flink-proto-confluent-<version>-all.jar` (Flink 1.20), `flink-proto-confluent-flink2-<version>-all.jar` (Flink 2.x) | Flink's `lib/` directory (SQL client, SQL gateway, session clusters) | Bundled. Nothing else to add except the Kafka SQL connector. |

Flink itself is in neither: the cluster or your job provides `flink-core` and `flink-table-common`.

!!! note "Releases up to 1.1.0"
    In 1.1.0 and earlier the unclassified JAR on Maven Central was the self-contained one, while its POM still listed every dependency. A build that depended on it got the bundled classes and the transitive dependencies at the same time. From the release after 1.1.0 the unclassified JAR is a plain library JAR and the self-contained JAR has the `all` classifier. If you copied the unclassified JAR into `lib/`, switch to the `-all` JAR when you upgrade.

## What the `-all` JAR contains

Libraries that often sit on a Flink classpath at a different version are relocated under `com.bbrownsound.flink.formats.proto.shaded`, so they cannot clash with a copy brought by Flink, another connector or your job:

| Library | Original package |
| --- | --- |
| Jackson | `com.fasterxml.jackson` |
| Guava | `com.google.common`, `com.google.thirdparty` |
| Gson | `com.google.gson` |
| Apache Commons (lang3, compress, codec, io) | `org.apache.commons` |
| Apache HttpComponents 5 | `org.apache.hc` |
| Avro | `org.apache.avro` |
| SnakeYAML | `org.yaml.snakeyaml` |
| Swagger annotations | `io.swagger` |

`com.google.type` (from `proto-google-common-protos`) is relocated to `shadowed.google.type`.

The following are bundled under their **original** package names, on purpose:

| Library | Package | Why it is not relocated |
| --- | --- | --- |
| protobuf-java | `com.google.protobuf` | Generated message classes you name in `message-class` link against it directly. |
| Confluent serializers and Schema Registry client | `io.confluent` | Options take these class names as strings, e.g. a subject name strategy of `io.confluent.kafka.serializers.subject.RecordNameStrategy`. |
| kafka-clients | `org.apache.kafka` | `dead-letter.properties` takes class names as strings, e.g. `org.apache.kafka.common.security.plain.PlainLoginModule` in `sasl.jaas.config`. |
| zstd-jni, snappy-java, lz4-java | `com.github.luben`, `org.xerial.snappy`, `net.jpountz` | Their native code is bound to the original package names. |
| Kotlin standard library, Okio, Wire | `kotlin`, `okio`, `com.squareup.wire` | Used by Confluent's protobuf schema parser; Kotlin metadata does not survive relocation. |
| Google common protos | `com.google.api`, `com.google.rpc`, … | Generated protobuf classes. |

The build enforces both lists: `./gradlew verifyJars` (part of `check`) fails if a relocated package appears under its original name, if Flink or SLF4J classes are bundled, or if the library JAR contains anything but this project's classes.

### What this means in practice

- **Kafka SQL connector.** `flink-sql-connector-kafka` relocates its own Kafka client under `org.apache.flink.kafka.shaded`, so it does not collide with the `kafka-clients` bundled here. The demo stack runs the `-all` JAR next to `flink-sql-connector-kafka` on `flink:1.20-java17` by default, and on `flink:2.2-java17` with `FLINK_VERSION=2.2`.
- **Another unrelocated `kafka-clients`, Confluent client or protobuf-java in `lib/`.** These will collide: two versions of the same classes on one classpath, and which one wins depends on file order. Keep only one, or use the library JAR in a job build where your build tool picks a single version.
- **Other Confluent serde JARs, even at the identical version.** The `-all` JAR cannot share a classpath with `kafka-avro-serializer` or any other JAR that brings the Confluent client under its original package. The bundled `io.confluent` and `org.apache.kafka` classes keep their names but are rewritten to reference the relocated Avro, Jackson and Guava, so their method signatures no longer match upstream and matching the Confluent version does not help. With the `-all` JAR first, the Avro serializer fails with `NoSuchMethodError: 'org.apache.avro.Schema io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils.getSchema(...)'`; with it last, this format fails the same way on a relocated Guava type. A job that needs both uses the library JAR and lets its build resolve one Confluent client.
- **Flink's `flink-sql-avro-confluent-registry` in `lib/`.** This works next to the `-all` JAR: Flink relocates its Confluent client, Kafka client and Avro under `org.apache.flink.avro...shaded`, so the two formats do not see each other's copies. The only classes both JARs bundle under the same name are zstd-jni and snappy-java.
- **JAAS login modules for the dead-letter producer.** Use the plain `org.apache.kafka...` class name in `dead-letter.properties`. The `org.apache.flink.kafka.shaded.` prefix belongs to the Kafka SQL connector's own client and applies only to the table's `properties.*` options.

## protobuf-java version

Both JARs use protobuf-java and protobuf-java-util 4.33.6. That is `protoVersion` in `gradle.properties`: the version the library JAR's POM declares, the one bundled in the `-all` JAR, and the one the format is compiled and tested against. It is also the minimum. An older 4.x runtime is not enough, because classes generated into `proto-google-common-protos` and the format's own code need it: with 4.31.1 the format fails with `NoClassDefFoundError: com/google/protobuf/GeneratedFile` as soon as it touches a schema.

The build keeps the declared version honest. `./gradlew verifyProtobufVersion` (part of `check`) fails if a dependency pulls protobuf-java or protobuf-java-util above `protoVersion`, and `declaredProtobufTest` runs the format with both pinned to exactly `protoVersion`. To see the exact version for a given checkout:

```bash
./gradlew dependencyInsight --dependency protobuf-java --configuration runtimeClasspath
```

This matters when you set `message-class`, because your generated classes run against that runtime:

- Classes generated by a `protoc` **newer** than the runtime are rejected by protobuf at class-initialisation time. Generate with a `protoc` at or below the runtime version.
- Classes generated by `protoc` 3.x fall under protobuf's cross-version rules for old generated code on a newer runtime; see the [protobuf cross-version runtime guarantee](https://protobuf.dev/support/cross-version-runtime-guarantee/) and test before relying on it.
- Tables that do not set `message-class` are unaffected: the format works from the schema in the registry and needs no generated classes.

### Aligning versions in a job build

Other Flink modules, `flink-protobuf` among them, bring protobuf-java 3.x. A job that depends on them and on this library ends up with two requested versions, and a build that resolves to 3.x fails at runtime with errors such as `NoSuchMethodError` or `NoClassDefFoundError: com/google/protobuf/RuntimeVersion`. Pin the 4.x version for the whole build, and pin protobuf-java-util with it so the two cannot drift apart.

=== "Gradle"

    ```kotlin
    configurations.all {
        resolutionStrategy.force(
            "com.google.protobuf:protobuf-java:4.33.6",
            "com.google.protobuf:protobuf-java-util:4.33.6",
        )
    }
    ```

=== "Maven"

    ```xml
    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>com.google.protobuf</groupId>
                <artifactId>protobuf-java</artifactId>
                <version>4.33.6</version>
            </dependency>
            <dependency>
                <groupId>com.google.protobuf</groupId>
                <artifactId>protobuf-java-util</artifactId>
                <version>4.33.6</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
    ```

Use the version this library declares, or newer; never an older one. If you build a fat job JAR, shade this library's dependencies the same way you shade your other connectors; the library JAR does not relocate anything itself.

### lz4-java on Flink 2.x

Flink 2.x depends on `at.yawk.lz4:lz4-java` (1.10.3 with Flink 2.2.1), a maintained fork of lz4-java that declares the `org.lz4:lz4-java` capability. This library reaches the original `org.lz4:lz4-java` (1.8.0) through Confluent's `kafka-clients`. Both provide the same `net.jpountz` classes, so the two must not end up on one classpath. Flink 1.20 uses `org.lz4:lz4-java` itself and is not affected.

Gradle sees the shared capability and refuses to pick one, so a job that depends on `flink-proto-confluent-flink2` and on `flink-clients` or `flink-runtime` 2.x fails to resolve:

```text
> Could not resolve at.yawk.lz4:lz4-java:1.10.3.
     Required by: root project > org.apache.flink:flink-clients:2.2.1 > org.apache.flink:flink-runtime:2.2.1
   > Module 'at.yawk.lz4:lz4-java' has been rejected:
        Cannot select module because of conflict with org.lz4:lz4-java:1.8.0 (runtime). Both provide capability 'org.lz4:lz4-java:1.10.3'.
```

Select Flink's fork. Maven has no notion of capabilities and silently puts both JARs on the classpath, so exclude the original there instead.

=== "Gradle (Kotlin)"

    ```kotlin
    configurations.configureEach {
        resolutionStrategy.capabilitiesResolution.withCapability("org.lz4", "lz4-java") {
            val fork = candidates.firstOrNull {
                val id = it.id
                id is ModuleComponentIdentifier && id.group == "at.yawk.lz4"
            }
            if (fork != null) {
                select(fork)
                because("Flink 2.x uses the at.yawk.lz4 fork of lz4-java")
            }
        }
    }
    ```

=== "Gradle (Groovy)"

    ```groovy
    configurations.configureEach {
        resolutionStrategy.capabilitiesResolution.withCapability('org.lz4', 'lz4-java') {
            def fork = candidates.find { it.id instanceof ModuleComponentIdentifier && it.id.group == 'at.yawk.lz4' }
            if (fork != null) {
                select(fork)
                because('Flink 2.x uses the at.yawk.lz4 fork of lz4-java')
            }
        }
    }
    ```

=== "Maven"

    ```xml
    <dependency>
        <groupId>com.bbrownsound</groupId>
        <artifactId>flink-proto-confluent-flink2</artifactId>
        <version>${flink-proto-confluent.version}</version>
        <exclusions>
            <exclusion>
                <groupId>org.lz4</groupId>
                <artifactId>lz4-java</artifactId>
            </exclusion>
        </exclusions>
    </dependency>
    ```

The rule only acts when the fork is present, so it is safe to keep in a build that also compiles against Flink 1.20.
