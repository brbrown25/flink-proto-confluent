# Handle bad records with a dead-letter topic

A record that is not valid Protobuf (or whose schema cannot be resolved) is a *poison record*. By default it fails the task. Set `on-deserialize-error` to `skip` to drop it, and add a `dead-letter-topic` to keep its raw bytes for inspection.

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

Each dead-letter record carries the original bytes plus `error.class`, `error.message` and `source.topic` headers. The `numDeserializeErrors` metric counts every record that failed to deserialize, `numDeadLetterRecords` counts the dead letters the broker acknowledged, and `numDeadLetterErrors` counts the ones that could not be written.

!!! warning "The dead-letter topic needs brokers"
    Put `bootstrap.servers` in `dead-letter.properties`. When it is missing, the format falls back to the table's own `properties.bootstrap.servers`, which is right when the dead-letter topic lives on the same cluster as the source topic. Only the broker list is inherited, so a secured cluster still needs its `security.protocol`, `sasl.*` or `ssl.*` settings in `dead-letter.properties`. With no brokers from either place the table is rejected with a `ValidationException` when the query is planned; it no longer degrades to a `WARN` and silently dropped records.

!!! warning "Quote values that contain a colon"
    Map options split each pair on `:`, so a `host:port` value must be quoted with doubled single quotes inside the SQL string, as in the example above. Unquoted, table creation fails with `Map item is not a key-value pair (missing ':'?)`.

!!! note "Only `fail` and `skip` are accepted"
    `on-deserialize-error` is matched case-insensitively, and any value other than `fail` or `skip` (including a typo such as `fial`) is rejected with a `ValidationException` when the query is planned. It no longer falls back to the skip path.

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

## Tracing a dead letter back to its source

A dead letter has the record's value bytes and the `source.topic` header, and nothing else about where it came from. The original key, partition, offset and timestamp are **not** on the dead letter, and the dead letter is written with a null key. This is a limit of Flink's format interface rather than a missing feature: a value format is handed only the value bytes (`DeserializationSchema.deserialize(byte[])`), and the Kafka connector does not pass the rest of the record to it.

What that means in practice:

- Dead letters cannot be replayed in their original order, or matched to an offset in the source topic, from the dead-letter topic alone.
- To find the source record, search the source topic for the value bytes around the time the dead letter was written (its own record timestamp is the time of the failure, not of the source record).
- If the producer of the source topic can put an identifier inside the payload, that identifier survives, because the payload is copied byte for byte.

A DataStream job that needs the origin can wrap its own `KafkaRecordDeserializationSchema`, which does see the whole `ConsumerRecord`, and write the dead letter itself. This project does not ship such a wrapper.

## When the producer is closed

The dead-letter producer is created when the source opens and is closed when the job leaves the TaskManager (finished, cancelled or failed for good). Flink's `DeserializationSchema` has no `close()` method, so the format registers a release hook on the job's user-code class loader and closes the producer there, waiting up to 10 seconds for dead letters that are still in flight. A task that fails because a `sync` dead-letter write failed closes its producer immediately.

One consequence: when a task restarts for another reason while the job stays on the same TaskManager, the previous attempt's producer stays open until the job leaves that TaskManager. It holds a network thread and its buffer, not correctness. Code that owns a `ProtoRowDataDeserializationSchema` directly can call its `close()` method.

Watch it happen in the [dead-letter demo](../demos.md#dead-letter-topic).
