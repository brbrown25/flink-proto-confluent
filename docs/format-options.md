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
| `on-deserialize-error` | `fail` | `fail` (throw and fail the task) or `skip` (log, count, drop the poison record). Matched case-insensitively; any other value (including a typo) is treated as `skip`. |
| `dead-letter-topic` | *(none)* | Topic that receives raw bytes of records that fail to deserialize. Requires `dead-letter.properties.bootstrap.servers`; without it no producer is created and failures are only logged and counted (a WARN is logged at open). |

SSL (`ssl.keystore.*`, `ssl.truststore.*`), auth (`basic-auth.*`,
`bearer-auth.*`), `properties`, and `dead-letter.properties` are also supported.

## Option precedence and validation

- **Typed options beat tunneled `properties`.** Anything set through the raw `properties` map is applied first, then the typed Flink options overwrite it. Setting both `properties.basic.auth.user.info` and `basic-auth.user-info` therefore resolves to the typed `basic-auth.user-info` value; the same holds for `basic-auth.credentials-source`, `bearer-auth.*`, `ssl.keystore.*`, `ssl.truststore.*`, and `message-class`. A tunneled key survives untouched only when its typed counterpart is unset.
- **`on-deserialize-error` is not validated.** Only `fail` (case-insensitive) is fatal; every other value, valid or not, takes the `skip` path — the error is logged, `numDeserializeErrors` is incremented, the record is optionally routed to the dead-letter topic, and `null` is returned. A typo therefore silently disables fail-fast behavior.
- **`url` and `topic` are required.** Omitting either makes format creation fail with a Flink `ValidationException` from `FactoryUtil.validateFactoryOptions` ("Missing required options"), for both the decoding and the encoding format.
- **A dead-letter topic without `bootstrap.servers` is a no-op.** `dead-letter-topic` alone does not fail the job; the producer is skipped and a WARN is logged.

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

## Schema evolution and subject naming

A source decodes each record with the writer schema named by the schema ID in the record's Confluent wire header, so a topic holding records written under several schema versions is read without extra configuration.

- **Columns added in a later version.** A table column that does not exist in the writer's schema is read as `NULL` (and logged once per converter at `WARN`), so a table declared against v2 still reads v1 records. The same warning is how a mistyped column name surfaces: it becomes an always-`NULL` column.
- **Columns removed in a later version.** Declare only the columns the table needs; columns the writer no longer has follow the rule above.
- **`use-schema-id`.** Pins the schema ID stamped into the wire header on a sink. The schema must already be registered under the subject.
- **`auto-register-schemas`.** With `false` (the default), writing to a subject that has no registered schema fails instead of silently registering one. With `true`, a change the registry rejects under the subject's compatibility level fails the job with the registry's error.
- **`normalize-schemas`.** The format always registers schemas derived from a protobuf descriptor, and those are already in Confluent's normalized form, so this option does not change what a sink registers. It does not retroactively match a denormalized schema that is already stored in the registry — that registers as a new version.
- **Subject-naming strategy.** There is no dedicated option; set it through the `properties` passthrough, e.g. `'value.proto-confluent.properties' = 'value.subject.name.strategy:io.confluent.kafka.serializers.subject.RecordNameStrategy'`. Strategies that derive the subject from the record rather than the topic require `message-class` or a Row-derived schema, since no subject can be named before a schema exists.
