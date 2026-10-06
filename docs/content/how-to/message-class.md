# Pin a message class

By default a sink derives a **dynamic** Protobuf schema from the Flink row type. Set `message-class` to a generated Protobuf class to register and serialize with that message's descriptor instead, so downstream consumers can read the topic with a strongly typed message.

```sql
CREATE TABLE keyed_sink (
  `k_id`     STRING,   -- key column (prefixed)
  `payload`  STRING,
  `event_ts` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',

  'key.format' = 'proto-confluent',
  'key.fields' = 'k_id',
  'key.fields-prefix' = 'k_',
  'key.proto-confluent.url' = 'http://schema-registry:8081',
  'key.proto-confluent.topic' = 'orders',
  'key.proto-confluent.is_key' = 'true',
  'key.proto-confluent.auto-register-schemas' = 'true',
  'key.proto-confluent.message-class' = 'com.example.OrderProto$OrderKey',

  'value.format' = 'proto-confluent',
  'value.fields-include' = 'EXCEPT_KEY',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.is_key' = 'false',
  'value.proto-confluent.auto-register-schemas' = 'true',
  'value.proto-confluent.message-class' = 'com.example.OrderProto$Order'
);
```

- `message-class` is scoped by `is_key`: on a key format it names the key message, on a value format the value message.
- The class must be on Flink's classpath (put your generated classes in a JAR in `lib/`).
- Use the JVM binary name — nested generated classes use `$`.
- Use `key.fields-prefix` when one message backs both key and value, so prefixed key columns still map to proto field names.
- Leave `message-class` unset to keep dynamic-schema behaviour.
