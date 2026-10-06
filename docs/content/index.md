# flink-proto-confluent

A [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) Protobuf format for Apache Flink SQL and the Table API. Read and write Kafka topics whose values (and keys) are Protobuf messages framed in the Confluent wire format, with the schema resolved from Schema Registry.

This project is an improved, standalone derivative of [amstee/flink-proto-confluent](https://github.com/amstee/flink-proto-confluent), repackaged under `com.bbrownsound`.

## What you get

- **A `proto-confluent` format** you reference from any Flink Kafka table: `'value.format' = 'proto-confluent'`.
- **Dynamic or explicit schemas.** Derive the Protobuf schema from the table's row type, or [pin a generated message class](how-to/message-class.md).
- **Poison-record handling.** [Skip bad records and route them to a dead-letter topic](how-to/dead-letter-topic.md) instead of crashing the job.
- **Secured registries.** [TLS, mutual TLS, basic auth and bearer-token auth](how-to/ssl-and-auth.md).
- **Schema evolution.** Records are decoded with the writer schema named in the wire header, so [mixed versions on one topic just work](how-to/schema-evolution.md).

## Where to go next

<div class="grid cards" markdown>

- **[Getting Started](getting-started.md)** — add the dependency and run your first query.
- **[Demos](demos.md)** — terminal recordings of the format in action.
- **[How-to Guides](how-to/message-class.md)** — task-oriented recipes.
- **[Configuration reference](reference/configuration.md)** — every option, default and precedence rule.

</div>
