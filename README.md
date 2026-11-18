# flink-proto-confluent

[![Maven Central](https://img.shields.io/maven-central/v/com.bbrownsound/flink-proto-confluent)](https://central.sonatype.com/artifact/com.bbrownsound/flink-proto-confluent)
[![Snapshots](https://img.shields.io/badge/snapshots-Sonatype%20Central-green)](https://central.sonatype.com/repository/maven-snapshots/)
[![CI](https://github.com/brbrown25/flink-proto-confluent/actions/workflows/ci.yml/badge.svg)](https://github.com/brbrown25/flink-proto-confluent/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/brbrown25/flink-proto-confluent/graph/badge.svg?token=WXE51L52H4)](https://codecov.io/gh/brbrown25/flink-proto-confluent)
[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://brbrown25.github.io/flink-proto-confluent/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GitHub release](https://img.shields.io/github/v/release/brbrown25/flink-proto-confluent)](https://github.com/brbrown25/flink-proto-confluent/releases)

A Confluent Schema Registry Protobuf format for Apache Flink Table API. **Documentation, how-to guides and terminal demos: <https://brbrown25.github.io/flink-proto-confluent/>.** This project is an improved, standalone derivative of [amstee/flink-proto-confluent](https://github.com/amstee/flink-proto-confluent), repackaged under `com.bbrownsound` with additional features and tests.

## Supported Flink versions

One release covers both Flink lines under one version number. Pick the artifact that matches your cluster's Flink line; the code and the `proto-confluent` format options are the same on both.

| Library version | Flink line | Tested Flink versions | Java | Kafka connector | artifactId |
| --- | --- | --- | --- | --- | --- |
| 1.3.0 and later | 1.20 | 1.20.5 (built against 1.20.0) | 17 or newer | `flink-sql-connector-kafka` / `flink-connector-kafka` `3.4.0-1.20` | `flink-proto-confluent` |
| 1.3.0 and later | 2.x | 2.2.1 | 17 or newer | `flink-sql-connector-kafka` / `flink-connector-kafka` `5.0.0-2.2` | `flink-proto-confluent-flink2` |
| 1.2.0 and earlier | 1.20 only | 1.20.0 | 17 or newer | `flink-sql-connector-kafka` / `flink-connector-kafka` `3.4.0-1.20` | `flink-proto-confluent` |

Both artifacts are published to Maven Central with the same group (`com.bbrownsound`) and version, each as a library JAR and a self-contained `-all` JAR:

| Flink line | Library JAR (job builds) | Self-contained JAR (Flink `lib/`) | Flink Docker image | Kafka SQL connector JAR for `lib/` |
| --- | --- | --- | --- | --- |
| 1.20 | `flink-proto-confluent-<version>.jar` | `flink-proto-confluent-<version>-all.jar` | `flink:1.20-java17` | `flink-sql-connector-kafka-3.4.0-1.20.jar` |
| 2.x | `flink-proto-confluent-flink2-<version>.jar` | `flink-proto-confluent-flink2-<version>-all.jar` | `flink:2.2-java17` | `flink-sql-connector-kafka-5.0.0-2.2.jar` |

The JARs are compiled for Java 17. Use a `-java17` (or newer) Flink image: the plain `flink:1.20` tag is Java 11 and fails with `UnsupportedClassVersionError`.

**Upgrading.** If you run Flink 1.20, nothing changes: keep the `flink-proto-confluent` artifactId and bump the version as usual. If you move a job or cluster to Flink 2.x, switch the artifactId to `flink-proto-confluent-flink2` (or swap the `-all` JAR in `lib/` for the `flink-proto-confluent-flink2-<version>-all.jar` one) at the same version, and replace the Kafka connector with its 2.x build. Never put both artifacts on one classpath.

## Resolving the dependency

Releases are published to Maven Central ([`flink-proto-confluent`](https://central.sonatype.com/artifact/com.bbrownsound/flink-proto-confluent) for Flink 1.20, [`flink-proto-confluent-flink2`](https://central.sonatype.com/artifact/com.bbrownsound/flink-proto-confluent-flink2) for Flink 2.x) as two JARs per Flink line:

- `<artifactId>-<version>.jar` — the library JAR, for a job you build with Gradle, Maven or sbt. Its dependencies are resolved by your build; Flink itself is expected to be provided.
- `<artifactId>-<version>-all.jar` — the self-contained JAR to drop into Flink's `lib/` directory (see [Usage](#usage)).

The library JAR depends on Confluent's serializers, which are hosted on Confluent's Maven repository rather than Maven Central, so **add `https://packages.confluent.io/maven/` next to `mavenCentral()`** or the build cannot resolve them. Add the dependency for your Flink line as follows.

### Flink 1.20

**Gradle (Groovy):**

<!-- x-release-please-start-version -->
```groovy
repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
    implementation 'com.bbrownsound:flink-proto-confluent:1.2.0'
}
```
<!-- x-release-please-end-version -->

**Gradle (Kotlin):**

<!-- x-release-please-start-version -->
```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://packages.confluent.io/maven/") }
}

dependencies {
    implementation("com.bbrownsound:flink-proto-confluent:1.2.0")
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
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.bbrownsound</groupId>
        <artifactId>flink-proto-confluent</artifactId>
        <version>1.2.0</version>
    </dependency>
</dependencies>
```
<!-- x-release-please-end-version -->

**sbt:**

<!-- x-release-please-start-version -->
```scala
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.2.0"
```
<!-- x-release-please-end-version -->

### Flink 2.x

`flink-proto-confluent-flink2` is published from 1.3.0 on; no earlier version of it exists.

**Gradle (Groovy):**

<!-- x-release-please-start-version -->
```groovy
repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
}

dependencies {
    implementation 'com.bbrownsound:flink-proto-confluent-flink2:1.2.0'
}
```
<!-- x-release-please-end-version -->

**Gradle (Kotlin):**

<!-- x-release-please-start-version -->
```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://packages.confluent.io/maven/") }
}

dependencies {
    implementation("com.bbrownsound:flink-proto-confluent-flink2:1.2.0")
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
    <repository>
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.bbrownsound</groupId>
        <artifactId>flink-proto-confluent-flink2</artifactId>
        <version>1.2.0</version>
    </dependency>
</dependencies>
```
<!-- x-release-please-end-version -->

**sbt:**

<!-- x-release-please-start-version -->
```scala
resolvers += "Confluent" at "https://packages.confluent.io/maven/"

libraryDependencies += "com.bbrownsound" % "flink-proto-confluent-flink2" % "1.2.0"
```
<!-- x-release-please-end-version -->

<!-- x-release-please-start-version -->
Replace `1.2.0` with the [latest release](https://github.com/brbrown25/flink-proto-confluent/releases) version.
<!-- x-release-please-end-version -->

### Snapshots

Every merge to `main` publishes a snapshot to the [Sonatype snapshot repository](https://central.sonatype.com/repository/maven-snapshots). Snapshot coordinates carry the short commit SHA they were built from — `1.0.1-a1b2c3d-SNAPSHOT`, not a bare `1.0.1-SNAPSHOT` — so the artifact you resolve today is the same one you resolved yesterday, and you can pin to the exact commit you tested against. The numeric part is the next patch after the last release; it is a placeholder for unreleased work on `main`, not a promise about the next version number.

Snapshots are published for both Flink lines under the same version. Browse the published snapshots for [Flink 1.20](https://central.sonatype.com/repository/maven-snapshots/com/bbrownsound/flink-proto-confluent/) or [Flink 2.x](https://central.sonatype.com/repository/maven-snapshots/com/bbrownsound/flink-proto-confluent-flink2/) to find the coordinate you want, then add the snapshot repository. The examples below use the Flink 1.20 artifactId; for Flink 2.x use `flink-proto-confluent-flink2` instead:

**Gradle (Groovy):**

```groovy
repositories {
    mavenCentral()
    maven { url 'https://packages.confluent.io/maven/' }
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
    maven { url = uri("https://packages.confluent.io/maven/") }
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
        <id>confluent</id>
        <url>https://packages.confluent.io/maven/</url>
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
resolvers += "Confluent" at "https://packages.confluent.io/maven/"
resolvers += "Sonatype Snapshots" at "https://central.sonatype.com/repository/maven-snapshots/"

libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.0.1-a1b2c3d-SNAPSHOT"
```

Substitute the SHA of the commit you want. Prefer a release version for stable builds; snapshots exist so you can try unreleased work with a coordinate that will not change underneath you.

## Build

```bash
./gradlew build shadowJar                       # Flink 1.20 (default, -PflinkVersion=1.20.0)
./gradlew build shadowJar -PflinkVersion=2.2.1  # Flink 2.x
```

`-PflinkVersion` selects the Flink line the build compiles and tests against, and with it the artifactId and the Kafka connector the tests use. See [CONTRIBUTING.md](CONTRIBUTING.md#building-and-testing) for the per-line test commands.

This produces both JARs in `build/libs/`: the library JAR and the self-contained `-all` JAR (`flink-proto-confluent-<version>[-all].jar` for Flink 1.20, `flink-proto-confluent-flink2-<version>[-all].jar` for Flink 2.x). `./gradlew check` also runs `verifyJars`, which fails the build if the library JAR bundles dependencies or the `-all` JAR contains Flink or unrelocated libraries.

Or use the Makefile (the targets build the default Flink 1.20 line):

- `make` / `make all` — full build (check + shadow JAR)
- `make build` — `./gradlew build shadowJar`
- `make test` — run tests
- `make check` — tests + checkstyle
- `make checkstyle` — checkstyle only
- `make coverage` — unit tests + integration tests + JaCoCo report (HTML in `build/reports/jacoco/test/html/`). Full `make check` enforces ≥80% line coverage overall and for packages `serialize` and `deserialize` (`jacocoTestCoverageVerification`). Tests do not use Mockito.
- `make clean` — `./gradlew clean`
- `make docs` — strict MkDocs build of the docs site into `build/docs-site/` (creates `.venv` from `docs/requirements.txt` on first use)
- `make docs-serve` — live docs preview at <http://127.0.0.1:8000>

## Releasing

Maintainers: see [docs/RELEASING.md](docs/RELEASING.md) for how to cut a release (tag a `v*` version → CI publishes to Maven Central and opens a GitHub Release) and how automatic snapshot publishing works.

## Usage

Copy the self-contained `-all` JAR for your Flink line into your Flink `lib/` directory, next to the matching Kafka SQL connector (see [Supported Flink versions](#supported-flink-versions)): `flink-proto-confluent-<version>-all.jar` on Flink 1.20, `flink-proto-confluent-flink2-<version>-all.jar` on Flink 2.x. Download it from Maven Central or the [GitHub Release](https://github.com/brbrown25/flink-proto-confluent/releases), or build it:

```bash
# Flink 1.20
./gradlew shadowJar
cp build/libs/flink-proto-confluent-[0-9]*-all.jar /path/to/flink/lib/

# Flink 2.x
./gradlew shadowJar -PflinkVersion=2.2.1
cp build/libs/flink-proto-confluent-flink2-*-all.jar /path/to/flink/lib/
```

Releases up to and including 1.1.0 published the self-contained JAR under the unclassified name (`flink-proto-confluent-1.1.0.jar`); the `-all` classifier starts with the release after that. See [Classpath and dependency conflicts](docs/content/reference/classpath.md) for what the JAR bundles and relocates, and which protobuf version your generated classes need.

Then in Flink SQL (e.g. `sql-client.sh`), use the format identifier `proto-confluent`:

```sql
'value.format' = 'proto-confluent',
'value.proto-confluent.url' = 'http://schema-registry:8081',
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

`message-class` is role-scoped by `is_key` (key vs value). See [the configuration reference](docs/content/reference/configuration.md) for the full option reference and a complete keyed-sink example.

## Documentation

The docs site lives in `docs/` (MkDocs Material) and is published to <https://brbrown25.github.io/flink-proto-confluent/>. Preview with `make docs-serve` and run the strict build CI uses with `make docs`; both use a virtualenv with the pinned `docs/requirements.txt`, because a globally installed `mkdocs` lacks the Material theme and fails with `Unrecognised theme name: 'material'`. The terminal demos are recorded with `docs/demo/record.sh`. See [Contributing](docs/content/contributing.md) for how to edit pages, re-record casts and what to update when options change.

## Contributing

Issues and pull requests are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md). AI-assisted contributions are welcome when a person has understood, verified and disclosed them; pull requests from unattended agents are closed without review. See the [AI usage policy](AI_POLICY.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## Testing

Tests use JUnit 5 and real components (e.g. Testcontainers for Kafka/Schema Registry); **Mockito is not used**. Coverage includes both unit tests and integration tests. CI runs the unit and integration suites on Flink 1.20.5 and 2.2.1; pass `-PflinkVersion=<version>` to run them locally against either line (see [CONTRIBUTING.md](CONTRIBUTING.md#building-and-testing)).

## Package layout

All code uses the **com.bbrownsound** package. Test protos in `src/test/proto/com/bbrownsound/` are generated with buf (managed mode disabled; `option java_package` in each proto). Run `make -C src/test/proto generate` to regenerate (requires `buf generate . --path com/`).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
