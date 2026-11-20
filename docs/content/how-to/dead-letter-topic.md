# Handle bad records with a dead-letter topic

A record that is not valid Protobuf (or whose schema cannot be resolved because the registry says it does not exist) is a *poison record*. By default it fails the task. Set `on-deserialize-error` to `skip` to drop it, and add a `dead-letter-topic` to keep its raw bytes for inspection. A Schema Registry that cannot be reached or answers with an error is not a poison record: it always fails the task, even with `skip` (see [When the Schema Registry is unavailable](#when-the-schema-registry-is-unavailable)).

```sql
CREATE TABLE orders (
  order_id STRING,
  amount   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',

  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.on-deserialize-error' = 'skip',
  'value.proto-confluent.dead-letter-topic' = 'orders-dlq',
  'value.proto-confluent.dead-letter.properties' = 'bootstrap.servers:''kafka:9092'''
);
```

Each dead-letter record carries the original bytes plus `error.class`, `error.message` and `source.topic` headers (see [Dead-letter headers](#dead-letter-headers) for what each one holds and when `source.topic` is left out). The `numDeserializeErrors` metric counts every record that failed to deserialize, `numDeadLetterRecords` counts the dead letters the broker acknowledged, and `numDeadLetterErrors` counts the ones that could not be written; the [metrics reference](../reference/metrics.md) lists every metric the format exposes.

!!! warning "The dead-letter topic needs brokers"
    Put `bootstrap.servers` in `dead-letter.properties`. When it is missing, the format falls back to the table's own `properties.bootstrap.servers`, which is right when the dead-letter topic lives on the same cluster as the source topic. Only the broker list is inherited, so a secured cluster still needs its `security.protocol`, `sasl.*` or `ssl.*` settings in `dead-letter.properties`. With no brokers from either place the table is rejected with a `ValidationException` when the query is planned; it no longer degrades to a `WARN` and silently dropped records.

!!! warning "Quote values that contain a colon"
    Map options split each pair on `:`, so a `host:port` value must be quoted with doubled single quotes inside the SQL string, as in the example above. Unquoted, table creation fails with `Map item is not a key-value pair (missing ':'?)`.

!!! note "Only `fail` and `skip` are accepted"
    `on-deserialize-error` is matched case-insensitively, and any value other than `fail` or `skip` (including a typo such as `fial`) is rejected with a `ValidationException` when the query is planned. It no longer falls back to the skip path.

## How bad records are logged

Each source logs its first poison record at `ERROR` with the full stack trace, then at most one more per minute, so that a topic full of bad records read with `skip` does not fill the TaskManager log with one stack trace per record. Every logged line carries `suppressedSinceLastLog`, the number of poison records the source read since the previous `ERROR` line without logging them. Those are logged at `DEBUG` with the exception's message and no stack trace, so set the `com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoRowDataDeserializationSchema` logger to `DEBUG` to see every one. Logging never decides what happens to a record: every poison record is still counted in `numDeserializeErrors` and sent to the dead-letter topic if one is set, so alert on the metric rather than on the log. With the default `fail` the first poison record fails the task, and the restarted task logs its own first failure in full.

## When the Schema Registry is unavailable

The source looks up the writer schema of every schema ID it has not seen yet in Schema Registry, and after a restart that is every schema ID. When the lookup fails because of the registry rather than the record, the task fails whatever `on-deserialize-error` says, and the record is not dropped, not counted in `numDeserializeErrors` and not sent to the dead-letter topic. Flink's restart strategy then restarts the job from its last checkpoint, and the record is read again once the registry is back. Without this, `skip` would drop (and dead-letter) every valid record read during an outage. The task fails with a `com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.SchemaRegistryUnavailableException` whose message names the schema ID, and the failed request is counted in `schemaRegistryErrors` (see the [metrics reference](../reference/metrics.md#schema-registry-metrics)).

| The registry lookup | Is | With `skip` |
| --- | --- | --- |
| Cannot connect, times out, or fails with another I/O error (unknown host, TLS) | An outage | The task fails. |
| Answers 5xx (500, 502, 503, 504, ...) | An outage | The task fails. |
| Answers 401 or 403 (for example after a credential rotation), 407, 408 or 429 | An outage | The task fails. |
| Answers 404 without the registry's own not-found error code (a proxy, or a wrong URL path) | An outage | The task fails. |
| Answers 404 with error code 40403 (schema not found), 40401 or 40402: the schema ID does not exist | A poison record | Dropped, counted and dead-lettered. |
| Answers any other 4xx | A poison record | Dropped, counted and dead-lettered. |

Records that never reach the registry, or that fail after their schema was resolved, are poison records as before: an unknown magic byte, a record too short for its header, a payload that is not valid Protobuf, a record of another message than `message-name` or `message-class`, and a record that cannot be converted to the table's column types.

The registry client retries 408, 429, 500, 502, 503 and 504 answers itself before it gives up; its `properties.max.retries`, `properties.retries.wait.ms` and `properties.retries.max.wait.ms` settings tune that. With the default `fail`, the only difference is that a record that failed on a registry outage is no longer counted in `numDeserializeErrors` or written to the dead-letter topic.

## Delivery guarantees

`dead-letter.delivery` decides what happens when the dead-letter write itself fails.

| Value | What the source does | If the dead-letter write fails |
| --- | --- | --- |
| `async` (default) | Hands the record to the producer and moves on. | The failure is logged at `ERROR` and counted in `numDeadLetterErrors`. The record is gone: it was neither emitted nor dead-lettered. |
| `sync` | Waits for the broker to acknowledge the dead letter before it moves on. | The task fails. The record is read again after the restart, so it is never silently dropped. |

```sql
  'value.proto-confluent.on-deserialize-error' = 'skip',
  'value.proto-confluent.dead-letter-topic' = 'orders-dlq',
  'value.proto-confluent.dead-letter.delivery' = 'sync',
  'value.proto-confluent.dead-letter.properties' = 'bootstrap.servers:''kafka:9092'',delivery.timeout.ms:30000'
```

Things to know before choosing `sync`:

- **Throughput.** Every poison record costs one produce round trip on the source thread. That is fine for the occasional bad record and slow for a topic that is mostly bad records.
- **How long it waits.** The wait is bounded by the producer's own timeouts, `max.block.ms` (60 seconds by default) and `delivery.timeout.ms` (2 minutes by default). Lower them in `dead-letter.properties` if the job should notice a dead-letter outage sooner.
- **The job stops while the dead-letter cluster is down.** That is the point: with a restart strategy the job keeps restarting and makes no progress past the poison record until the dead letter can be written.
- **Duplicates are possible.** Delivery is at-least-once. A record that was dead-lettered after the last checkpoint is dead-lettered again when the job restarts from that checkpoint. Set `enable.idempotence` and `acks` in `dead-letter.properties` as you would for any producer.

With `async`, alert on `numDeadLetterErrors`: any increase means a poison record was dropped without a dead letter.

`dead-letter.delivery` is matched case-insensitively and accepts only `async` and `sync`; anything else is rejected with a `ValidationException` when the query is planned.

## Dead-letter headers

| Header | Value | When it is set |
| --- | --- | --- |
| `error.class` | Fully qualified class name of the exception that made the record fail, for example `org.apache.kafka.common.errors.SerializationException`. | Always. |
| `error.message` | That exception's message (`null` when it has none). | Always. |
| `source.topic` | The source table's topic: the format's `topic` option, or else the Kafka connector's single `topic`. | Only when the source has such a topic. |

The format is handed only a record's value bytes, never the topic the Kafka connector read it from, so `source.topic` is the topic the table is configured with, not one read from the record. A source that names its schema with `subject` and has no single topic, for example a table that reads with `topic-pattern` or a list of topics, has no topic to report: its dead letters have no `source.topic` header at all, rather than a misleading one (before this, the header held the string `null`). Use the dead-letter topic itself, one per source table, to tell such sources apart.

## Tracing a dead letter back to its source

A dead letter has the record's value bytes and the `source.topic` header (when the source has a single topic, see [Dead-letter headers](#dead-letter-headers)), and nothing else about where it came from. The original key, partition, offset and timestamp are **not** on the dead letter, and the dead letter is written with a null key. This is a limit of Flink's format interface rather than a missing feature: a value format is handed only the value bytes (`DeserializationSchema.deserialize(byte[])`), and the Kafka connector does not pass the rest of the record to it.

What that means in practice:

- Dead letters cannot be replayed in their original order, or matched to an offset in the source topic, from the dead-letter topic alone.
- To find the source record, search the source topic for the value bytes around the time the dead letter was written (its own record timestamp is the time of the failure, not of the source record).
- If the producer of the source topic can put an identifier inside the payload, that identifier survives, because the payload is copied byte for byte.

A DataStream job that needs the origin can wrap its own `KafkaRecordDeserializationSchema`, which does see the whole `ConsumerRecord`, and write the dead letter itself. This project does not ship such a wrapper.

## When the producer is closed

The dead-letter producer is created when the source opens and is closed when the job leaves the TaskManager (finished, cancelled or failed for good). Flink's `DeserializationSchema` has no `close()` method, so the format registers a release hook on the job's user-code class loader and closes the producer there, waiting up to 10 seconds for dead letters that are still in flight. A task that fails because a `sync` dead-letter write failed closes its producer immediately.

One consequence: when a task restarts for another reason while the job stays on the same TaskManager, the previous attempt's producer stays open until the job leaves that TaskManager. It holds a network thread and its buffer, not correctness. Code that owns a `ProtoRowDataDeserializationSchema` directly can call its `close()` method.

Watch it happen in the [dead-letter demo](../demos.md#dead-letter-topic).
