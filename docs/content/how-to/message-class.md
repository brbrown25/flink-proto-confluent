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
- The `key.proto-confluent.topic` and `value.proto-confluent.topic` lines can be omitted here, because the format falls back to the table's single `topic`. Set `subject` or `subject-name-strategy` on either format to change the subject the message is registered under; see [Subject naming](schema-evolution.md#subject-naming).
- On a source, `message-class` checks the table and the records instead; see [read with a message class](#read-with-a-message-class).

## Read with a message class

On a source, `message-class` does not change how records are decoded: each record is still read with the writer schema named by its schema ID, and columns are matched to fields by name. It adds two checks instead.

- **The table must match the message.** Every column must exist in the message (by exact name, or its `camelCase` form for a `snake_case` column) and have a type that can be read from that field, recursively through `ROW`, `ARRAY` and `MAP` columns. A column may also use one of the [declared alternative types](../reference/type-mapping.md#declared-alternatives) for its field. A `oneof` column's fields are checked against the cases they read (by name, or by position for a field that names no case; see [type mapping](../reference/type-mapping.md#derived-types)), and a field with no case is a problem such as `column 'payload.other' has no case in oneof 'com.example.Order.payload' (cases: …)`. Message fields the table does not declare are fine. A mismatch fails the query with a `ValidationException` naming each offending column, such as `column 'order_id' has no field in message 'com.example.Order' (fields: id, amount)` or `column 'amount' is INT but field 'com.example.Order.amount' (int64) reads as BIGINT`. The check runs when the query is planned if the planner can load the class (for example from `lib/` or a JAR added with `ADD JAR`), and otherwise when the source starts on the TaskManager.
- **Only the columns a query reads are converted.** When the connector pushes the query's projection down to the format, the source is built for the projected columns, and the check above covers those columns; see [projection pushdown](../reference/configuration.md#projection-pushdown). When the connector pushes a nested projection, the check covers the projected paths: for `payload.a`, `payload` must be a message field (or a oneof) of the message and `a` a field of it that reads as its column's type, and the other fields declared in `payload` are not checked. Problems name the path, such as `column 'payload.added_later' has no field in message 'com.example.Order.Payload' (fields: …)`.
- **Records must be of that message.** A record whose writer message has a different full name than the class's message is a deserialize error and follows [`on-deserialize-error`](../reference/configuration.md): by default the job fails; with `skip` the record is dropped, counted in `numDeserializeErrors` and sent to the `dead-letter-topic` if one is set, with `error.class` = `com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.UnexpectedMessageTypeException`. When a writer schema defines several messages, the message the record was written with (from its message indexes) is the one compared.

```sql
CREATE TABLE orders (
  `id`     STRING,
  `amount` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.message-class' = 'com.example.OrderProto$Order',
  'value.proto-confluent.on-deserialize-error' = 'skip'
);
```

The table is checked against the class, not against each writer schema version: a record written under an older version that lacks a field the class has still reads that column as `NULL`, as described in [schema evolution](schema-evolution.md).

### Only check the message name

To reject records of other message types without putting generated classes on Flink's classpath (for example in a SQL-only deployment), set `message-name` to the message's fully-qualified protobuf name instead. Only the record check above applies; the columns are not checked.

```sql
  'value.proto-confluent.message-name' = 'com.example.Order'
```

- `message-name` is the protobuf name (`package.Message`, nested messages as `package.Outer.Inner`), not the JVM class name with `$`.
- With both options set, they must name the same message, or the table is rejected.
- `message-name` only affects a source. A sink ignores it; pin what a sink writes with `message-class`.
