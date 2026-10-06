# Getting Started

## 1. Get the JAR

Flink loads formats from its `lib/` directory. The JAR is compiled for **Java 17**, so run Flink on JDK 17 or newer (for example the `flink:1.20-java17` image; the default `flink:1.20` image is Java 11 and fails with `UnsupportedClassVersionError`). Either build the shadow JAR or resolve it from Maven Central.

<!-- x-release-please-start-version -->
=== "Build from source"

    ```bash
    ./gradlew shadowJar
    cp build/libs/flink-proto-confluent-*.jar /path/to/flink/lib/
    ```

=== "Gradle (Kotlin)"

    ```kotlin
    repositories { mavenCentral() }

    dependencies {
        implementation("com.bbrownsound:flink-proto-confluent:1.1.0")
    }
    ```

=== "Gradle (Groovy)"

    ```groovy
    repositories { mavenCentral() }

    dependencies {
        implementation 'com.bbrownsound:flink-proto-confluent:1.1.0'
    }
    ```

=== "Maven"

    ```xml
    <dependency>
        <groupId>com.bbrownsound</groupId>
        <artifactId>flink-proto-confluent</artifactId>
        <version>1.1.0</version>
    </dependency>
    ```

=== "sbt"

    ```scala
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
