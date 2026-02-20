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

```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation 'com.bbrownsound:flink-proto-confluent:1.0.0'
}
```

**Gradle (Kotlin):**

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("com.bbrownsound:flink-proto-confluent:1.0.0")
}
```

**Maven:**

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
        <version>1.0.0</version>
    </dependency>
</dependencies>
```

**sbt:**

```scala
libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.0.0"
```

Replace `1.0.0` with the [latest release](https://github.com/bbrownsound/flink-proto-confluent/releases) version.

### Snapshots

Snapshot builds (e.g. `1.0.0-SNAPSHOT`) are published to the [Sonatype snapshot repository](https://central.sonatype.com/repository/maven-snapshots) on each merge to `main`. To depend on snapshots, add that repository and use a `-SNAPSHOT` version.

**Gradle (Groovy):**

```groovy
repositories {
    mavenCentral()
    maven { url 'https://central.sonatype.com/repository/maven-snapshots/' }
}

dependencies {
    implementation 'com.bbrownsound:flink-proto-confluent:1.0.0-SNAPSHOT'
}
```

**Gradle (Kotlin):**

```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://central.sonatype.com/repository/maven-snapshots/") }
}

dependencies {
    implementation("com.bbrownsound:flink-proto-confluent:1.0.0-SNAPSHOT")
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
        <version>1.0.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

**sbt:**

```scala
resolvers += "Sonatype Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.0.0-SNAPSHOT"
```

Use the current snapshot version from `build.gradle.kts` (or the repo); resolution may use a timestamped build. Prefer a release version for stable builds.

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

## Testing

Tests use JUnit 5 and real components (e.g. Testcontainers for Kafka/Schema Registry); **Mockito is not used**. Coverage includes both unit tests and integration tests.

## Package layout

All code uses the **com.bbrownsound** package. Test protos in `src/test/proto/com/bbrownsound/` are generated with buf (managed mode disabled; `option java_package` in each proto). Run `make -C src/test/proto generate` to regenerate (requires `buf generate . --path com/`).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
