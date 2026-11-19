# flink-proto-confluent

A [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) Protobuf format for Apache Flink SQL and the Table API. Read and write Kafka topics whose values (and keys) are Protobuf messages framed in the Confluent wire format, with the schema resolved from Schema Registry.

This project is an improved, standalone derivative of [amstee/flink-proto-confluent](https://github.com/amstee/flink-proto-confluent), repackaged under `com.bbrownsound`.

## What you get

- **A `proto-confluent` format** you reference from any Flink Kafka table: `'value.format' = 'proto-confluent'`.
- **Dynamic or explicit schemas.** Derive the Protobuf schema from the table's row type, or [pin a generated message class](how-to/message-class.md).
- **Poison-record handling.** [Skip bad records and route them to a dead-letter topic](how-to/dead-letter-topic.md) instead of crashing the job.
- **Secured registries.** [TLS, mutual TLS, basic auth and bearer-token auth](how-to/ssl-and-auth.md).
- **Schema evolution.** Records are decoded with the writer schema named in the wire header, so [mixed versions on one topic just work](how-to/schema-evolution.md).

## Why this format

Open-source Flink ships two formats near this one, and neither reads Protobuf topics written by Confluent's serializers:

| | Flink [`protobuf`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/protobuf/) | Flink [`avro-confluent`](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/formats/avro-confluent/) | `proto-confluent` (this library) |
| --- | --- | --- | --- |
| Serialization | Protobuf | Avro | Protobuf |
| Confluent Schema Registry | No | Yes | Yes |
| Confluent wire format (magic byte, schema ID) | No: plain Protobuf bytes | Yes | Yes, with Protobuf message indexes |
| Generated classes on the classpath | Required (`protobuf.message-class-name`) | Not applicable | Not needed: schemas come from the registry ([`message-class`](how-to/message-class.md) is optional) |
| Records written with several schema versions | Read with the one compiled class | Each record read with its writer schema | Each record read with its writer schema ([schema evolution](how-to/schema-evolution.md)) |

Use `proto-confluent` when your topics hold Protobuf records produced with Confluent's `KafkaProtobufSerializer` (or anything else that writes the Confluent wire format), and you want Flink SQL, the Table API or the [DataStream API](how-to/datastream-api.md) to read and write them without compiling the `.proto` files into your job.

### What it adds over upstream

This library started from [amstee/flink-proto-confluent](https://github.com/amstee/flink-proto-confluent), which provides the `proto-confluent` format itself: dynamic schemas from the registry, the Protobuf ↔ Flink type mapping and the Schema Registry connection and authentication options. Since then this project has added:

- **Flink 2.x.** One release, two artifacts: `flink-proto-confluent` for Flink 1.20 and `flink-proto-confluent-flink2` for Flink 2.x (see [Getting Started](getting-started.md)).
- **Poison-record handling.** `on-deserialize-error` = `skip` and a [dead-letter topic](how-to/dead-letter-topic.md), with `async` or `sync` delivery.
- **Pinned message classes** for the key and value of a sink, and `message-class` / `message-name` checks on a source ([Pin a message class](how-to/message-class.md)).
- **Subject naming.** `subject` and `subject-name-strategy` options, and `topic` falls back to the table's topic ([Subject naming](how-to/schema-evolution.md#subject-naming)).
- **DDL generation.** A [`ddl` command](how-to/generate-ddl.md) in the `-all` JAR prints the `CREATE TABLE` for a registered subject.
- **A Schema Registry catalog** that exposes every Protobuf topic as a table ([Query topics through a catalog](how-to/catalog.md)).
- **Projection pushdown.** Sources decode only the columns, and nested fields, a query reads.
- **[Metrics](reference/metrics.md)** for deserialize and serialize errors, dead-letter records, Schema Registry calls and cached schemas.
- **Wider type coverage**: `TIMESTAMP` for `google.protobuf.Timestamp`, `DECIMAL(20, 0)` for unsigned 64-bit integers, JSON strings for `Struct` / `Value` / `ListValue`, enums as their number, `INTERVAL DAY TO SECOND` for `google.protobuf.Duration` and recursive messages ([Type mapping](reference/type-mapping.md)).
- **Sink schema handling.** A sink writes under the registered schema's ID, fails on Schema Registry errors instead of silently switching to a derived schema, and can refresh its schema with `schema-cache.ttl`.
- **Packaging.** A thin library JAR for job builds and a self-contained `-all` JAR with conflicting libraries relocated ([Classpath and dependency conflicts](reference/classpath.md)).
- **Tests** against real Kafka and Schema Registry containers, including TLS, mutual TLS, basic and bearer authentication, schema evolution and both Flink lines.

## Where to go next

<div class="grid cards" markdown>

- **[Getting Started](getting-started.md)** — pick the artifact for Flink 1.20 or 2.x, add the dependency and run your first query.
- **[Demos](demos.md)** — terminal recordings of the format in action.
- **[How-to Guides](how-to/message-class.md)** — task-oriented recipes.
- **[Configuration reference](reference/configuration.md)** — every option, default and precedence rule.
- **[Metrics reference](reference/metrics.md)** — every metric the format exposes, with its type and meaning.
- **[Type mapping](reference/type-mapping.md)** — which SQL type to declare for each protobuf field, in both directions, and the nullability rules.
- **[DataStream API](how-to/datastream-api.md)** — a `KafkaSource` / `KafkaSink` example without SQL.

</div>
