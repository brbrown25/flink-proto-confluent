# Getting Started

## 1. Get the JAR

Flink loads formats from its `lib/` directory. The JAR is compiled for **Java 17**, so run Flink on JDK 17 or newer (for example the `flink:1.20-java17` image; the default `flink:1.20` image is Java 11 and fails with `UnsupportedClassVersionError`). There are two JARs, for two ways of running Flink:

- **SQL client, SQL gateway, or any cluster where formats live in `lib/`:** use the self-contained `-all` JAR. It bundles the Confluent client and its dependencies (see [Classpath and dependency conflicts](reference/classpath.md)).
- **A job you build yourself (Table API or DataStream):** depend on the library JAR and let your build resolve its dependencies.

### Self-contained JAR for `lib/`

<!-- x-release-please-start-version -->
=== "Download"

    ```bash
    curl -fLO https://repo1.maven.org/maven2/com/bbrownsound/flink-proto-confluent/1.1.0/flink-proto-confluent-1.1.0-all.jar
    cp flink-proto-confluent-1.1.0-all.jar /path/to/flink/lib/
    ```

=== "Build from source"

    ```bash
    ./gradlew shadowJar
    cp build/libs/flink-proto-confluent-*-all.jar /path/to/flink/lib/
    ```
<!-- x-release-please-end-version -->

!!! note "Releases up to 1.1.0"
    The `-all` classifier starts with the release after 1.1.0. In 1.1.0 and earlier the self-contained JAR was published under the unclassified name, `flink-proto-confluent-<version>.jar`; download that file instead. The same JARs are attached to each [GitHub Release](https://github.com/brbrown25/flink-proto-confluent/releases).

### Library dependency for a job build

The library depends on Confluent's serializers, which are hosted on Confluent's Maven repository and not on Maven Central. Add that repository or the build cannot resolve them. Flink itself is not a transitive dependency: your job (or the cluster) provides it.

<!-- x-release-please-start-version -->
=== "Gradle (Kotlin)"

    ```kotlin
    repositories {
        mavenCentral()
        maven { url = uri("https://packages.confluent.io/maven/") }
    }

    dependencies {
        implementation("com.bbrownsound:flink-proto-confluent:1.1.0")
    }
    ```

=== "Gradle (Groovy)"

    ```groovy
    repositories {
        mavenCentral()
        maven { url 'https://packages.confluent.io/maven/' }
    }

    dependencies {
        implementation 'com.bbrownsound:flink-proto-confluent:1.1.0'
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
            <version>1.1.0</version>
        </dependency>
    </dependencies>
    ```

=== "sbt"

    ```scala
    resolvers += "Confluent" at "https://packages.confluent.io/maven/"

    libraryDependencies += "com.bbrownsound" % "flink-proto-confluent" % "1.1.0"
    ```
<!-- x-release-please-end-version -->

<!-- x-release-please-start-version -->
Replace `1.1.0` with the [latest release](https://github.com/brbrown25/flink-proto-confluent/releases). Unreleased work on `main` is published as SHA-qualified snapshots such as `1.0.1-a1b2c3d-SNAPSHOT` from the [Sonatype snapshot repository](https://central.sonatype.com/repository/maven-snapshots/).
<!-- x-release-please-end-version -->

!!! note "Flink Kafka connector"
    The format decodes and encodes bytes; you still need the Flink Kafka SQL connector on the classpath to read from or write to Kafka.

## 2. Declare a table

Use the format identifier `proto-confluent`. `url` and `topic` are required; `topic` is used to derive the subject (`<topic>-value`).

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

## 3. Query it

```sql
SELECT order_id, customer, amount FROM orders;
```

To write, add `'value.proto-confluent.auto-register-schemas' = 'true'` so the schema derived from the row type is registered on first write, then `INSERT INTO orders SELECT ...`.

See the [demos](demos.md) for this flow recorded end to end, and the [configuration reference](reference/configuration.md) for every option.
