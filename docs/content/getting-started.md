# Getting Started

## Supported Flink versions

One release covers both Flink lines under one version number. Pick the artifact that matches your cluster's Flink line; the code and the `proto-confluent` format options are the same on both.

| Library version | Flink line | Tested Flink versions | Java | Kafka connector | artifactId |
| --- | --- | --- | --- | --- | --- |
| 1.3.0 and later | 1.20 | 1.20.5 | 17 or newer | `flink-sql-connector-kafka` / `flink-connector-kafka` `3.4.0-1.20` | `flink-proto-confluent` |
| 1.3.0 and later | 2.x | 2.2.1 | 17 or newer | `flink-sql-connector-kafka` / `flink-connector-kafka` `5.0.0-2.2` | `flink-proto-confluent-flink2` |
| 1.2.0 and earlier | 1.20 only | 1.20.0 | 17 or newer | `flink-sql-connector-kafka` / `flink-connector-kafka` `3.4.0-1.20` | `flink-proto-confluent` |

Both artifacts are published to Maven Central with the same group (`com.bbrownsound`) and version, each as a library JAR and a self-contained `-all` JAR:

| Flink line | Library JAR (job builds) | Self-contained JAR (Flink `lib/`) | Flink Docker image | Kafka SQL connector JAR for `lib/` |
| --- | --- | --- | --- | --- |
| 1.20 | `flink-proto-confluent-<version>.jar` | `flink-proto-confluent-<version>-all.jar` | `flink:1.20-java17` | `flink-sql-connector-kafka-3.4.0-1.20.jar` |
| 2.x | `flink-proto-confluent-flink2-<version>.jar` | `flink-proto-confluent-flink2-<version>-all.jar` | `flink:2.2-java17` | `flink-sql-connector-kafka-5.0.0-2.2.jar` |

The JARs are compiled for Java 17. Use a `-java17` (or newer) Flink image: the plain `flink:1.20` tag is Java 11 and fails with `UnsupportedClassVersionError`.

**Upgrading.** If you run Flink 1.20, nothing changes: keep the `flink-proto-confluent` artifactId and bump the version as usual. If you move a job or cluster to Flink 2.x, switch the artifactId to `flink-proto-confluent-flink2` (or swap the `-all` JAR in `lib/` for the `flink-proto-confluent-flink2-<version>-all.jar` one) at the same version, and replace the Kafka connector with its 2.x build. Never put both artifacts on one classpath.

## 1. Get the JAR

Flink loads formats from its `lib/` directory. There are two JARs per Flink line, for two ways of running Flink:

- **SQL client, SQL gateway, or any cluster where formats live in `lib/`:** use the self-contained `-all` JAR. It bundles the Confluent client and its dependencies (see [Classpath and dependency conflicts](reference/classpath.md)).
- **A job you build yourself (Table API or DataStream):** depend on the library JAR and let your build resolve its dependencies.

The tabs below have a Flink 1.20 and a Flink 2.x variant; pick the one that matches your cluster (see [Supported Flink versions](#supported-flink-versions)).

### Self-contained JAR for `lib/`

Download the `-all` JAR for your Flink line from Maven Central and copy it next to the Kafka SQL connector:

<!-- x-release-please-start-version -->
=== "Flink 1.20"

    ```bash
    VERSION=1.3.0
    curl -fLO "https://repo1.maven.org/maven2/com/bbrownsound/flink-proto-confluent/$VERSION/flink-proto-confluent-$VERSION-all.jar"
    cp "flink-proto-confluent-$VERSION-all.jar" /path/to/flink/lib/
    ```

=== "Flink 2.x"

    ```bash
    VERSION=1.3.0
    curl -fLO "https://repo1.maven.org/maven2/com/bbrownsound/flink-proto-confluent-flink2/$VERSION/flink-proto-confluent-flink2-$VERSION-all.jar"
    cp "flink-proto-confluent-flink2-$VERSION-all.jar" /path/to/flink/lib/
    ```
<!-- x-release-please-end-version -->

Or build it from source, with `-PflinkVersion` selecting the line:

=== "Flink 1.20"

    ```bash
    ./gradlew shadowJar
    cp build/libs/flink-proto-confluent-[0-9]*-all.jar /path/to/flink/lib/
    ```

=== "Flink 2.x"

    ```bash
    ./gradlew shadowJar -PflinkVersion=2.2.1
    cp build/libs/flink-proto-confluent-flink2-*-all.jar /path/to/flink/lib/
    ```

!!! note "Releases up to 1.1.0"
    The `-all` classifier starts with the release after 1.1.0. In 1.1.0 and earlier the self-contained JAR was published under the unclassified name, `flink-proto-confluent-<version>.jar`; download that file instead. The same JARs are attached to each [GitHub Release](https://github.com/brbrown25/flink-proto-confluent/releases). `flink-proto-confluent-flink2` is published from 1.3.0 on; no earlier version of it exists.

### Library dependency for a job build

The library depends on Confluent's serializers, which are hosted on Confluent's Maven repository and not on Maven Central. Add that repository or the build cannot resolve them. Flink itself is not a transitive dependency: your job (or the cluster) provides it.

<!-- x-release-please-start-version -->
=== "Flink 1.20"

    === "Gradle (Kotlin)"

        ```kotlin
        repositories {
            mavenCentral()
            maven { url = uri("https://packages.confluent.io/maven/") }
        }

        dependencies {
            implementation("com.bbrownsound:flink-proto-confluent:1.3.0")
        }
        ```

    === "Gradle (Groovy)"

        ```groovy
        repositories {
            mavenCentral()
            maven { url 'https://packages.confluent.io/maven/' }
        }

        dependencies {
            implementation 'com.bbrownsound:flink-proto-confluent:1.3.0'
        }
        ```

    === "Maven"

        ```xml
        <repositories>
            <repository>
                <id>confluent</id>
                <url>https://packages.confluent.io/maven/</url>
            </repository>
        </repositories>

        <dependencies>
            <dependency>
                <groupId>com.bbrownsound</groupId>
                <artifactId>flink-proto-confluent</artifactId>
                <version>1.3.0</version>
            </dependency>
        </dependencies>
        ```

    === "sbt"

        ```scala
        resolvers += "Confluent" at "https://packages.confluent.io/maven/"

        libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.3.0"
        ```

=== "Flink 2.x"

    === "Gradle (Kotlin)"

        ```kotlin
        repositories {
            mavenCentral()
            maven { url = uri("https://packages.confluent.io/maven/") }
        }

        dependencies {
            implementation("com.bbrownsound:flink-proto-confluent-flink2:1.3.0")
        }
        ```

    === "Gradle (Groovy)"

        ```groovy
        repositories {
            mavenCentral()
            maven { url 'https://packages.confluent.io/maven/' }
        }

        dependencies {
            implementation 'com.bbrownsound:flink-proto-confluent-flink2:1.3.0'
        }
        ```

    === "Maven"

        ```xml
        <repositories>
            <repository>
                <id>confluent</id>
                <url>https://packages.confluent.io/maven/</url>
            </repository>
        </repositories>

        <dependencies>
            <dependency>
                <groupId>com.bbrownsound</groupId>
                <artifactId>flink-proto-confluent-flink2</artifactId>
                <version>1.3.0</version>
            </dependency>
        </dependencies>
        ```

    === "sbt"

        ```scala
        resolvers += "Confluent" at "https://packages.confluent.io/maven/"

        libraryDependencies += "com.bbrownsound" % "flink-proto-confluent-flink2" % "1.3.0"
        ```
<!-- x-release-please-end-version -->

<!-- x-release-please-start-version -->
Replace `1.3.0` with the [latest release](https://github.com/brbrown25/flink-proto-confluent/releases). Unreleased work on `main` is published as SHA-qualified snapshots such as `1.0.1-a1b2c3d-SNAPSHOT` from the [Sonatype snapshot repository](https://central.sonatype.com/repository/maven-snapshots/), for both artifactIds.
<!-- x-release-please-end-version -->

!!! note "Flink Kafka connector"
    The format decodes and encodes bytes; you still need the Flink Kafka SQL connector for your Flink line on the classpath to read from or write to Kafka: `flink-sql-connector-kafka-3.4.0-1.20.jar` on Flink 1.20, `flink-sql-connector-kafka-5.0.0-2.2.jar` on Flink 2.x.

!!! warning "Gradle builds on Flink 2.x"
    Flink 2.x and this library bring two different lz4-java modules that Gradle refuses to resolve side by side (`Both provide capability 'org.lz4:lz4-java'`). Add the capability rule from [lz4-java on Flink 2.x](reference/classpath.md#lz4-java-on-flink-2x); on Maven, exclude `org.lz4:lz4-java` from this dependency.

## 2. Declare a table

Use the format identifier `proto-confluent`. `url` is required, and so is a topic or a subject: `topic` is used to derive the subject (`<topic>-value`).

```sql
CREATE TABLE orders (
  order_id STRING,
  customer STRING,
  amount   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',

  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.is_key' = 'false'
);
```

`value.proto-confluent.topic` can be omitted when the table's Kafka `topic` names a single topic, as here: the format then derives the subject from that topic. To name the subject directly instead, replace it with `'value.proto-confluent.subject' = 'orders-value'`. See [Subject naming](how-to/schema-evolution.md#subject-naming).

!!! tip "Generate the statement instead"
    For a schema that is already registered, `java -jar flink-proto-confluent-<version>-all.jar ddl --url http://schema-registry:8081 --subject orders-value` prints this statement with every column, nested `ROW`, `ARRAY` and `MAP` filled in from the schema. See [Generate table DDL](how-to/generate-ddl.md).

## 3. Query it

```sql
SELECT order_id, customer, amount FROM orders;
```

To write, add `'value.proto-confluent.auto-register-schemas' = 'true'` so the schema derived from the row type is registered on first write, then `INSERT INTO orders SELECT ...`.

See the [demos](demos.md) for this flow recorded end to end, and the [configuration reference](reference/configuration.md) for every option.
