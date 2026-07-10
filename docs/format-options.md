# proto-confluent format options

Options for the `proto-confluent` Flink Table format. Prefix each with the role
namespace in `CREATE TABLE` — e.g. `value.proto-confluent.url` or
`key.proto-confluent.url`.

| Option | Default | Description |
| --- | --- | --- |
| `url` | *(required)* | Confluent Schema Registry URL (fallback key: `schema-registry.url`). |
| `topic` | *(required)* | Kafka topic used to derive the schema subject (`<topic>-key` / `<topic>-value`). |
| `is_key` | `false` | Whether this format instance encodes/decodes the Kafka key. Set `true` on a `key.format`. |
| `message-class` | *(none)* | Fully-qualified generated protobuf message class used for serialization and schema registration instead of a dynamic (Row-derived) schema. Applies to the key when `is_key` is `true` and to the value otherwise. |
| `auto-register-schemas` | `false` | Register the schema with the registry on write. |
| `normalize-schemas` | `true` | Normalize schemas before registration/lookup. |
| `use-schema-id` | `-1` | Fixed schema ID for serialization (`-1` = auto). |
| `skip-known-types` | `true` | Skip well-known types during schema handling. |
| `on-deserialize-error` | `fail` | `fail` (throw and fail the task) or `skip` (log, count, drop the poison record). |
| `dead-letter-topic` | *(none)* | Topic that receives raw bytes of records that fail to deserialize. |

SSL (`ssl.keystore.*`, `ssl.truststore.*`), auth (`basic-auth.*`,
`bearer-auth.*`), `properties`, and `dead-letter.properties` are also supported.

## Explicit key and value message classes

By default a sink derives a **dynamic** protobuf schema from the Flink `Row`
type. To instead pin an **explicit named entity** (a generated protobuf message
class) for the key and/or the value, set `message-class` on the corresponding
format. The format registers and serializes with that message's descriptor, so
downstream consumers can read the topic with a specific (strongly typed)
protobuf type.

```sql
CREATE TABLE keyed_sink (
  `k_id`      STRING,   -- key column (prefixed)
  `payload`   STRING,   -- value columns
  `event_ts`  STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',

  -- Key: explicit named message class
  'key.format' = 'proto-confluent',
  'key.fields' = 'k_id',
  'key.fields-prefix' = 'k_',
  'key.proto-confluent.url' = 'http://schema-registry:8081',
  'key.proto-confluent.topic' = 'orders',
  'key.proto-confluent.is_key' = 'true',
  'key.proto-confluent.auto-register-schemas' = 'true',
  'key.proto-confluent.message-class' = 'com.example.OrderProto$OrderKey',

  -- Value: explicit named message class
  'value.format' = 'proto-confluent',
  'value.fields-include' = 'EXCEPT_KEY',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.is_key' = 'false',
  'value.proto-confluent.auto-register-schemas' = 'true',
  'value.proto-confluent.message-class' = 'com.example.OrderProto$Order'
)
```

Notes:

- `message-class` is role-scoped by `is_key`: on a `key.format`
  (`is_key = true`) it sets the key message class; on a `value.format` it sets
  the value message class.
- Use `key.fields-prefix` when the same protobuf message backs both the key and
  the value, so the prefixed key columns still map to the proto field names once
  the prefix is stripped.
- When `message-class` is unset the format keeps the previous dynamic-schema
  behavior.
- The class name is the JVM binary name — nested/generated message classes use
  `$` (e.g. `com.example.OrderProto$Order`).
