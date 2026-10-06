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
