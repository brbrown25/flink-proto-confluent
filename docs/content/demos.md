# Demos

Real terminal recordings, captured against a local Kafka + Schema Registry + Flink 1.20 stack (`flink:1.20-java17` with `flink-sql-connector-kafka-3.4.0-1.20`). The format and its options behave the same on Flink 2.x; see [`docs/demo`](https://github.com/brbrown25/flink-proto-confluent/tree/main/docs/demo) to run the stack yourself on either line (`FLINK_VERSION=2.2` runs it on `flink:2.2-java17`). Click play, or pause and copy text straight from the terminal.

## Quickstart

Protobuf `Order` records sit on a topic in the Confluent wire format. A Flink table declared with `'value.format' = 'proto-confluent'` reads them and aggregates with plain SQL.

In the recording, the format JAR's `ddl` command [generates that table](how-to/generate-ddl.md) from the registered `orders-value` subject instead of it being written by hand.

<div class="cast" data-src="../assets/casts/quickstart.cast" data-rows="32"></div>

The equivalent hand-written DDL, which [`sql/quickstart.sql`](https://github.com/brbrown25/flink-proto-confluent/blob/main/docs/demo/sql/quickstart.sql) in the demo stack uses:

```sql
CREATE TABLE orders (
  order_id STRING,
  customer STRING,
  amount   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'docs-demo',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.is_key' = 'false'
);
```

## Writing Protobuf from Flink

An `INSERT INTO` a `proto-confluent` table with `auto-register-schemas` registers a schema derived from the row type, and the records are readable by any Confluent Protobuf consumer.

<div class="cast" data-src="../assets/casts/write-back.cast" data-rows="32"></div>

For a schema generated from your own `.proto`, [pin a message class](how-to/message-class.md) instead.

## Dead-letter topic

A plain-text record lands on a Protobuf topic. With `on-deserialize-error = skip` and a `dead-letter-topic`, the query still returns both valid rows and the poison record's raw bytes are kept on the DLQ with `error.class`, `error.message` and `source.topic` headers. See the [how-to](how-to/dead-letter-topic.md).

<div class="cast" data-src="../assets/casts/dead-letter.cast" data-rows="32"></div>

## Schema evolution: compatible change, then a breaking one

A subject starts at v1, gains a field in a compatible v2, then a v3 that retypes `amount` from `double` to `string` is rejected by the registry (HTTP 409, `FIELD_SCALAR_KIND_CHANGED`) and never becomes a version. A table declared against v2 reads both writer schemas; the v1 row gets `NULL` for the new column. See [Schema evolution](how-to/schema-evolution.md#lifecycle-initial-compatible-breaking).

<div class="cast" data-src="../assets/casts/schema-evolution.cast" data-rows="32"></div>

## Breaking change and the dead-letter topic

With compatibility set to `NONE` the registry lets a breaking schema through. A table still declared against the old types cannot decode the new records: with `on-deserialize-error = skip` and a `dead-letter-topic` the valid rows still come back and the incompatible record's raw bytes land on the DLQ with `Unsupported protobuf type: STRING for a SQL type: DOUBLE` as the error. See [the how-to](how-to/schema-evolution.md#when-a-breaking-change-gets-through).

<div class="cast" data-src="../assets/casts/breaking-change-dlq.cast" data-rows="32"></div>

## Compatibility modes

The same breaking change checked under `BACKWARD`, `FORWARD`, `FULL` and `NONE`, then the difference between `BACKWARD` and `BACKWARD_TRANSITIVE` when an older version is the one a new schema conflicts with.

The registry's `/compatibility/.../versions/latest` check is not transitive, so it reports the BACKWARD_TRANSITIVE-rejected schema as compatible; only registration (or a check against each older version) catches it. See [the warning](how-to/schema-evolution.md#compatibility-modes). The demo helper `check --all-versions` tests every registered version.

<div class="cast" data-src="../assets/casts/compat-modes.cast" data-rows="32"></div>
