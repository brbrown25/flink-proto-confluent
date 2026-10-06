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

Each dead-letter record carries the original bytes plus `error.class`, `error.message` and `source.topic` headers. The `numDeserializeErrors` metric counts every skipped record.

!!! warning "`bootstrap.servers` is required for the topic to be written"
    Without `dead-letter.properties` containing `bootstrap.servers`, no producer is created: failures are only logged and counted, and a `WARN` is logged at open.

!!! warning "Quote values that contain a colon"
    Map options split each pair on `:`, so a `host:port` value must be quoted with doubled single quotes inside the SQL string, as in the example above. Unquoted, table creation fails with `Map item is not a key-value pair (missing ':'?)`.

!!! warning "Only `fail` is fatal"
    `on-deserialize-error` is matched case-insensitively and **any value other than `fail`** — including a typo — takes the skip path.

Watch it happen in the [dead-letter demo](../demos.md#dead-letter-topic).
