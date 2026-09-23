# flink-proto-confluent

[![Snapshots](https://img.shields.io/badge/snapshots-Sonatype%20Central-green)](https://central.sonatype.com/repository/maven-snapshots/)
[![CI](https://github.com/brbrown25/flink-proto-confluent/actions/workflows/ci.yml/badge.svg)](https://github.com/brbrown25/flink-proto-confluent/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/brbrown25/flink-proto-confluent/graph/badge.svg?token=WXE51L52H4)](https://codecov.io/gh/brbrown25/flink-proto-confluent)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub release](https://img.shields.io/github/v/release/brbrown25/flink-proto-confluent)](https://github.com/brbrown25/flink-proto-confluent/releases)

A Confluent Schema Registry Protobuf format for Apache Flink Table API. This project is an improved, standalone derivative of [amstee/flink-proto-confluent](https://github.com/amstee/flink-proto-confluent), repackaged under `com.bbrownsound` with additional features and tests.

## Resolving the dependency

Releases are published to [Maven Central](https://search.maven.org/artifact/com.bbrownsound/flink-proto-confluent). Add the dependency as follows.

**Gradle (Groovy):**

<!-- x-release-please-start-version -->
```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation 'com.bbrownsound:flink-proto-confluent:1.1.0'
}
```
<!-- x-release-please-end-version -->

**Gradle (Kotlin):**

<!-- x-release-please-start-version -->
```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("com.bbrownsound:flink-proto-confluent:1.1.0")
}
```
<!-- x-release-please-end-version -->

**Maven:**

<!-- x-release-please-start-version -->
```xml
<repositories>
    <repository>
        <id>central</id>
        <url>https://repo.maven.apache.org/maven2</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.bbrownsound</groupId>
        <artifactId>flink-proto-confluent</artifactId>
        <version>1.1.0</version>
    </dependency>
</dependencies>
```
<!-- x-release-please-end-version -->

**sbt:**

<!-- x-release-please-start-version -->
```scala
libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.1.0"
```
<!-- x-release-please-end-version -->

<!-- x-release-please-start-version -->
Replace `1.1.0` with the [latest release](https://github.com/bbrownsound/flink-proto-confluent/releases) version.
<!-- x-release-please-end-version -->

### Snapshots

Every merge to `main` publishes a snapshot to the [Sonatype snapshot repository](https://central.sonatype.com/repository/maven-snapshots). Snapshot coordinates carry the short commit SHA they were built from — `1.0.1-a1b2c3d-SNAPSHOT`, not a bare `1.0.1-SNAPSHOT` — so the artifact you resolve today is the same one you resolved yesterday, and you can pin to the exact commit you tested against. The numeric part is the next patch after the last release; it is a placeholder for unreleased work on `main`, not a promise about the next version number.

Browse [the published snapshots](https://central.sonatype.com/repository/maven-snapshots/com/bbrownsound/flink-proto-confluent/) to find the coordinate you want, then add the snapshot repository:

**Gradle (Groovy):**

```groovy
repositories {
    mavenCentral()
    maven { url 'https://central.sonatype.com/repository/maven-snapshots/' }
}

dependencies {
    implementation 'com.bbrownsound:flink-proto-confluent:1.0.1-a1b2c3d-SNAPSHOT'
}
```

**Gradle (Kotlin):**

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://central.sonatype.com/repository/maven-snapshots/") }
}

dependencies {
    implementation("com.bbrownsound:flink-proto-confluent:1.0.1-a1b2c3d-SNAPSHOT")
}
```

**Maven:**

```xml
<repositories>
    <repository>
        <id>central</id>
        <url>https://repo.maven.apache.org/maven2</url>
    </repository>
    <repository>
        <id>sonatype-snapshots</id>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <snapshots><enabled>true</enabled></snapshots>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.bbrownsound</groupId>
        <artifactId>flink-proto-confluent</artifactId>
        <version>1.0.1-a1b2c3d-SNAPSHOT</version>
    </dependency>
</dependencies>
```

**sbt:**

```scala
resolvers += "Sonatype Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.0.1-a1b2c3d-SNAPSHOT"
```

Substitute the SHA of the commit you want. Prefer a release version for stable builds; snapshots exist so you can try unreleased work with a coordinate that will not change underneath you.

## Build

```bash
./gradlew build shadowJar
```

Or use the Makefile:

- `make` / `make all` — full build (check + shadow JAR)
- `make build` — `./gradlew build shadowJar`
- `make test` — run tests
- `make check` — tests + checkstyle
- `make checkstyle` — checkstyle only
- `make coverage` — unit tests + integration tests + JaCoCo report (HTML in `build/reports/jacoco/test/html/`). Full `make check` enforces ≥80% line coverage overall and for packages `serialize` and `deserialize` (`jacocoTestCoverageVerification`). Tests do not use Mockito.
- `make clean` — `./gradlew clean`

## Releasing

Maintainers: see [docs/RELEASING.md](docs/RELEASING.md) for how to cut a release (tag a `v*` version → CI publishes to Maven Central and opens a GitHub Release) and how automatic snapshot publishing works.

## Usage

Copy the built JAR into your Flink `lib/` directory:

```bash
cp build/libs/proto-confluent.jar /path/to/flink/lib/
```

Then in Flink SQL (e.g. `sql-client.sh`), use the format identifier `proto-confluent`:

```sql
'value.format' = 'proto-confluent',
'value.proto-confluent.url' = 'http://schema-registry:8081',
'value.proto-confluent.topic' = 'your-topic',
'value.proto-confluent.is_key' = 'false'
```

To pin an **explicit named** protobuf message class for the key and/or value (instead of the default dynamic, Row-derived schema), set `message-class` on the corresponding format:

```sql
'key.format' = 'proto-confluent',
'key.proto-confluent.is_key' = 'true',
'key.proto-confluent.message-class' = 'com.example.OrderProto$OrderKey',
'value.format' = 'proto-confluent',
'value.proto-confluent.message-class' = 'com.example.OrderProto$Order'
```

`message-class` is role-scoped by `is_key` (key vs value). See [docs/format-options.md](docs/format-options.md) for the full option reference and a complete keyed-sink example.

## Testing

Tests use JUnit 5 and real components (e.g. Testcontainers for Kafka/Schema Registry); **Mockito is not used**. Coverage includes both unit tests and integration tests.

## Package layout

All code uses the **com.bbrownsound** package. Test protos in `src/test/proto/com/bbrownsound/` are generated with buf (managed mode disabled; `option java_package` in each proto). Run `make -C src/test/proto generate` to regenerate (requires `buf generate . --path com/`).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
