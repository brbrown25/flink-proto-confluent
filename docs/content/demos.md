# Demos

Real terminal recordings, captured against a local Kafka + Schema Registry + Flink 1.20 stack (see [`docs/demo`](https://github.com/brbrown25/flink-proto-confluent/tree/main/docs/demo) to run it yourself). Click play, or pause and copy text straight from the terminal.

## Quickstart

Protobuf `Order` records sit on a topic in the Confluent wire format. A Flink table declared with `'value.format' = 'proto-confluent'` reads them and aggregates with plain SQL.

<div class="cast" data-src="../assets/casts/quickstart.cast" data-rows="32"></div>

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

<div class="cast" data-src="../assets/casts/compat-modes.cast" data-rows="32"></div>
