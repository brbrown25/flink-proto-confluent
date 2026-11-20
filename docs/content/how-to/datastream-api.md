# Use the DataStream API

The format's runtime classes are public, so a DataStream job can read and write Confluent-framed Protobuf records without the Table API: `ProtoRowDataDeserializationSchema` is a `DeserializationSchema<RowData>` for a `KafkaSource`, and `ProtoRowDataSerializationSchema` a `SerializationSchema<RowData>` for a `KafkaSink`. Records are Flink's internal `RowData`, converted exactly as for a table with the same row type (see [Type mapping](../reference/type-mapping.md)).

## Dependencies

Add the library for your Flink line (see [Getting Started](../getting-started.md)) and the Kafka connector, `flink-connector-kafka` (`3.4.0-1.20` on Flink 1.20, `5.0.0-2.2` on Flink 2.x). The example also uses `DataTypes` from `flink-table-common` and `InternalTypeInfo` from `flink-table-runtime`; a Flink distribution has both in `lib/`, so declare them `provided` (`compileOnly`) at your cluster's Flink version.

## Example

The class below builds a source and a sink for the same row type and connects them in a job that copies one topic to another. It compiles unchanged against Flink 1.20 and Flink 2.x, and runs in the build's integration tests against Kafka and Schema Registry containers on both.

```java
--8<-- "src/integration-test/java/com/bbrownsound/flink/formats/proto/registry/confluent/docs/DataStreamExample.java:example"
```

Run it like any DataStream job:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamExample.copy(env, "kafka:9092", "http://schema-registry:8081", "orders", "orders-copy");
env.execute("copy orders");
```

## How it fits together

- **The row type is the table schema.** Each `RowType` field is read from, and written to, the Protobuf field of the same name, as a table column would be. Generate it from a registered subject with the [`ddl` command](generate-ddl.md) and translate the column types, or build it with `DataTypes` as above.
- **`ProtoConfluentFormatConfig`** holds the format options. Building it from a Flink `Configuration` keyed by `ProtoConfluentFormatOptions`, as `formatConfig` does, reads them the way a table reads its `value.proto-confluent.*` options, defaults included, so every option in the [configuration reference](../reference/configuration.md) (authentication, `subject`, `message-class`, `on-deserialize-error`, the dead-letter topic, …) is set the same way, without the prefix. There is no table to fall back on: set `topic` (or `subject`) yourself, and with a dead-letter topic put `bootstrap.servers` in `dead-letter.properties`. For a record key, build a second config with `ProtoConfluentFormatOptions.IS_KEY` set to `true` and pass its serialization schema to the record serializer's `setKeySerializationSchema`.
- **The source** needs the row type's `TypeInformation<RowData>`: `InternalTypeInfo.of(rowType)`. Each record is decoded with the writer schema named in its header, so records written under different schema versions can share a topic ([Schema evolution](schema-evolution.md)). A record that fails to decode fails the job unless `on-deserialize-error` is `skip` ([dead-letter topic](dead-letter-topic.md)).
- **The sink** writes with the latest schema registered under the topic's subject. Only while the subject does not exist does it derive a schema from the row type, and it registers that schema only when `auto-register-schemas` is `true` (the example turns it on for the sink alone). Pin a generated class with `message-class` to write with that message instead ([Pin a message class](message-class.md)).
- **Metrics.** Both schemas register the format's [metrics](../reference/metrics.md) on the operator's metric group, as they do for a table.
- **Closing.** Both schemas create their Schema Registry client when they open and close it, with any credential provider it holds, when the job leaves the TaskManager, through a release hook on the job's user-code class loader (the source closes its dead-letter producer there too). Both are `AutoCloseable`, so code that opens a schema outside a Flink job can call `close()` itself.
