# Configuration

Options for the `proto-confluent` Flink Table format. Prefix each with the role
namespace in `CREATE TABLE` — e.g. `value.proto-confluent.url` or
`key.proto-confluent.url`.

| Option | Default | Description |
| --- | --- | --- |
| `url` | *(required)* | Confluent Schema Registry URL (fallback key: `schema-registry.url`). |
| `topic` | the table's `topic` | Kafka topic used to derive the schema subject (`<topic>-key` / `<topic>-value` with the default strategy). When unset, the Kafka connector's own `topic` option is used if it names exactly one topic. Not needed when `subject` is set. |
| `subject` | *(none)* | Schema Registry subject to register and look up schemas under, used as is, e.g. `orders-value`. Replaces the subject-naming strategy, so `topic` is not required. Cannot be combined with `subject-name-strategy`. |
| `subject-name-strategy` | *(none)*, which applies `topic` | How the subject is derived: `topic` (`<topic>-key` / `<topic>-value`), `record` (the fully-qualified message name), `topic-record` (`<topic>-<message name>`), matched case-insensitively, or the fully-qualified name of a class implementing Confluent's `SubjectNameStrategy`. Applies to the key when `is_key` is `true` and to the value otherwise. Fallback key: `subject_name_strategy`. |
| `is_key` | `false` | Whether this format instance encodes/decodes the Kafka key. Set `true` on a `key.format`. |
| `message-class` | *(none)* | Fully-qualified generated protobuf message class used for serialization and schema registration instead of a dynamic (Row-derived) schema. Applies to the key when `is_key` is `true` and to the value otherwise. On a source, the table's columns are checked against the message and records of any other message type follow `on-deserialize-error`; see [read with a message class](../how-to/message-class.md#read-with-a-message-class). Fallback key: `message_class`. |
| `message-name` | *(none)* | Source only. Fully-qualified protobuf message name (`com.example.Order`) that every record's writer message must have; a record of any other message is a deserialize error and follows `on-deserialize-error`. Needs no class on the classpath. With `message-class` also set, both must name the same message. Ignored by a sink, but a malformed value is rejected on either. Fallback key: `message_name`. |
| `auto-register-schemas` | `false` | Register the schema with the registry on write. Fallback key: `auto_register_schemas`. |
| `normalize-schemas` | `true` | Normalize schemas before registration/lookup. Fallback key: `normalize_schemas`. |
| `use-schema-id` | `-1` | Fixed schema ID for serialization (`-1` = auto). Fallback key: `use_schema_id`. |
| `skip-known-types` | `true` | Skip well-known types during schema handling. Fallback key: `skip_known_types`. |
| `schema-cache.ttl` | *(none)* | Sink only. How long the latest schema looked up for the subject is reused before the registry is asked again, as a Flink duration (`30 s`, `5 min`); must be positive. Unset, the sink keeps the schema from its first write until the job restarts. Fallback key: `schema_cache.ttl`. See [how a sink picks its schema](../how-to/schema-evolution.md#how-a-sink-picks-its-schema-and-how-long-it-keeps-it). |
| `on-deserialize-error` | `fail` | `fail` (throw and fail the task) or `skip` (log, count, drop the poison record). A Schema Registry that cannot be reached or fails (I/O error, 5xx, 401, 403, 429, ...) fails the task with either value, without counting or dead-lettering the record; a schema ID the registry says does not exist (404, error code 40403) is a poison record. See [When the Schema Registry is unavailable](../how-to/dead-letter-topic.md#when-the-schema-registry-is-unavailable). Allowed values: `fail`, `skip`, matched case-insensitively. Any other value (including a typo or an empty string) is rejected with a `ValidationException`. The fallback key `on_deserialize_error` is also accepted. |
| `dead-letter-topic` | *(none)* | Topic that receives raw bytes of records that fail to deserialize. Needs brokers: `bootstrap.servers` in `dead-letter.properties`, or failing that the table's own `properties.bootstrap.servers`. With neither, the table is rejected with a `ValidationException`. Fallback key: `dead_letter_topic`. |
| `dead-letter.properties` | *(none)* | Map of properties passed verbatim to the dead-letter `KafkaProducer`: `bootstrap.servers`, security settings, and producer tuning such as `max.block.ms` or `delivery.timeout.ms`. Only used when `dead-letter-topic` is set. |
| `dead-letter.delivery` | `async` | `async` (hand the record to the producer and continue; a failed send is logged and counted, but the record may be lost) or `sync` (wait for the broker to acknowledge the record before the source moves on; a failed send fails the task). Matched case-insensitively; any other value is rejected with a `ValidationException`. Fallback key: `dead_letter.delivery`. See [delivery guarantees](../how-to/dead-letter-topic.md#delivery-guarantees). |
| `ssl.keystore.location`, `ssl.keystore.password` | *(none)* | Keystore file and its password for mutual TLS to the Schema Registry (the client's `schema.registry.ssl.keystore.*`). See [SSL and authentication](../how-to/ssl-and-auth.md). |
| `ssl.truststore.location`, `ssl.truststore.password` | *(none)* | Truststore file and its password for TLS to the Schema Registry (the client's `schema.registry.ssl.truststore.*`). |
| `basic-auth.credentials-source`, `basic-auth.user-info` | *(none)* | HTTP Basic authentication to the Schema Registry, e.g. `USER_INFO` and `user:password` (the client's `basic.auth.credentials.source` and `basic.auth.user.info`). |
| `bearer-auth.credentials-source`, `bearer-auth.token` | *(none)* | Bearer-token authentication to the Schema Registry, e.g. `STATIC_TOKEN` and the token (the client's `bearer.auth.credentials.source` and `bearer.auth.token`). |
| `properties` | *(none)* | Map of any other Schema Registry client properties, passed through as is, e.g. `'value.proto-confluent.properties' = 'key:value'`. Typed options win over the same setting passed here; see [precedence](#option-precedence-and-validation). |

SSL (`ssl.keystore.*`, `ssl.truststore.*`), auth (`basic-auth.*`,
`bearer-auth.*`) and `properties` are also supported.

## Option precedence and validation

- **Typed options beat tunneled `properties`.** Anything set through the raw `properties` map is applied first, then the typed Flink options overwrite it. Setting both `properties.basic.auth.user.info` and `basic-auth.user-info` therefore resolves to the typed `basic-auth.user-info` value; the same holds for `basic-auth.credentials-source`, `bearer-auth.*`, `ssl.keystore.*`, `ssl.truststore.*`, and `message-class`. `subject` and `subject-name-strategy` likewise overwrite a `key.subject.name.strategy` or `value.subject.name.strategy` set through `properties`. A tunneled key survives untouched only when its typed counterpart is unset.
- **`on-deserialize-error` is validated.** The only allowed values are `fail` and `skip`, in any letter case, under either `on-deserialize-error` or the fallback key `on_deserialize_error`. Anything else makes format creation fail with a Flink `ValidationException` that names the allowed values, for example `Invalid value 'fial' for option 'on-deserialize-error'. Allowed values (case-insensitive): fail, skip.` The check runs when the planner builds the table source or sink, so a typo fails the query before any record is read. With `skip` the error is logged, `numDeserializeErrors` is incremented, the record is optionally routed to the dead-letter topic, and `null` is returned. A Schema Registry outage is not a deserialize error: it fails the task even with `skip`.
- **Upgrading from 1.1.0 or earlier:** `on-deserialize-error` used to be a free-form string where every value other than `fail` silently behaved as `skip`. A table that relied on some other value (for example `ignore` or `drop`) to skip poison records now fails validation; change it to `skip`.
- **`url` is required.** Omitting it makes format creation fail with a Flink `ValidationException` from `FactoryUtil.validateFactoryOptions` ("Missing required options"), for both the decoding and the encoding format.
- **A topic or a subject is required.** The format needs `topic`, `subject`, or a Kafka connector `topic` that names exactly one topic to fall back to. A table with none of them (for example a source declared with `topic-pattern` or a list of topics, and no `subject`) is rejected with a `ValidationException` that says which of them is missing. A format `topic` wins over the connector's.
- **`subject` and `subject-name-strategy` are mutually exclusive.** An explicit subject is used as is, so a strategy next to it would be silently ignored; setting both is rejected with a `ValidationException`. A blank `subject` is rejected too.
- **`subject-name-strategy` is validated.** A value other than `topic`, `record` or `topic-record` must name a class that can be loaded and implements `io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy`; otherwise the query fails when it is planned with a `ValidationException` that lists the allowed values.
- **A dead-letter topic needs brokers.** When `dead-letter-topic` is set and `dead-letter.properties` has no `bootstrap.servers`, the format uses the table's `properties.bootstrap.servers` (the Kafka connector's option). Only the broker list is inherited: security settings for the dead-letter producer still go in `dead-letter.properties`. If the table has no `properties.bootstrap.servers` either, format creation fails with a `ValidationException`, for the decoding and the encoding format alike. A schema built in code without the format factory fails in `open()` with an `IllegalStateException` instead.
- **`dead-letter.delivery` is validated.** The only allowed values are `async` and `sync`, in any letter case, under either `dead-letter.delivery` or the fallback key `dead_letter.delivery`. Anything else fails with `Invalid value 'sink' for option 'dead-letter.delivery'. Allowed values (case-insensitive): async, sync.` when the query is planned; it is never treated as `async`.
- **`message-class` and `message-name` are checked on a source.** A `message-name` that is not a dot-separated list of identifiers (for example the JVM name `com.example.OrderProto$Order`) fails with a `ValidationException` when the query is planned. With `message-class`, a table column that has no field in the message, or whose type cannot be read from that field, fails with a `ValidationException` naming every offending column, for example `column 'order_id' has no field in message 'com.example.Order' (fields: id, amount)`. The check runs when the query is planned if the planner can load the class, and otherwise when the source opens on the TaskManager. A `message-name` that names a different message than `message-class` is rejected the same way.
- **Upgrading from 1.1.0 or earlier:** a `dead-letter-topic` with no brokers used to log a WARN at open and then drop poison records without dead-lettering them. Such a table now either picks up the table's `properties.bootstrap.servers` and starts writing dead letters, or fails validation. Add `bootstrap.servers` to `dead-letter.properties`, or remove `dead-letter-topic`.

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
- The `key.proto-confluent.topic` and `value.proto-confluent.topic` lines can be omitted, because the format falls back to the table's single `topic`, or replaced by `subject` to name each subject directly.

## Schema evolution and subject naming

A source decodes each record with the writer schema named by the schema ID in the record's Confluent wire header, so a topic holding records written under several schema versions is read without extra configuration.

- **Columns added in a later version.** A table column that does not exist in the writer's schema is read as `NULL` (and logged once per converter at `WARN`), so a table declared against v2 still reads v1 records. The same warning is how a mistyped column name surfaces: it becomes an always-`NULL` column.
- **Columns removed in a later version.** Declare only the columns the table needs; columns the writer no longer has follow the rule above.
- **`use-schema-id`.** Pins the schema ID stamped into the wire header on a sink. The schema must already be registered under the subject.
- **`auto-register-schemas`.** With `false` (the default), writing to a subject that has no registered schema fails instead of silently registering one. With `true`, a change the registry rejects under the subject's compatibility level fails the job with the registry's error.
- **Registry errors on a sink.** A sink without `message-class` looks up the latest schema of its subject. Only a "subject not found" answer (error code `40401`) makes it derive a schema from the table (and register it when `auto-register-schemas` is `true`). Every other failure, including `401`, `403`, `5xx`, timeouts, "version not found" (`40402`) and a 404 without that error code, fails the write with the registry's error and registers nothing.
- **`schema-cache.ttl`.** Unset, a sink keeps the schema it resolved at its first write until the job restarts. Set, the sink looks the subject up again once the schema is older than the TTL and switches to a new version if there is one; if that refresh fails, it keeps the schema it has and retries. `0` and negative durations are rejected with a `ValidationException`. Details in [how a sink picks its schema](../how-to/schema-evolution.md#how-a-sink-picks-its-schema-and-how-long-it-keeps-it).
- **`normalize-schemas`.** The format always registers schemas derived from a protobuf descriptor, and those are already in Confluent's normalized form, so this option does not change what a sink registers. It does not retroactively match a denormalized schema that is already stored in the registry — that registers as a new version.
- **Subject-naming strategy.** Set it with the `subject-name-strategy` option, e.g. `'value.proto-confluent.subject-name-strategy' = 'record'`, or, as before, through the `properties` passthrough, e.g. `'value.proto-confluent.properties' = 'value.subject.name.strategy:io.confluent.kafka.serializers.subject.RecordNameStrategy'`. The passthrough still works; when both are set, the typed option wins. Strategies that derive the subject from the record rather than the topic require `message-class` or a Row-derived schema, since no subject can be named before a schema exists. The Row-derived message is `com.flink.proto.confluent.Row`, so with `record` it registers under the subject `com.flink.proto.confluent.Row`.
- **Explicit subject.** `subject` names the subject outright instead of deriving it from the topic. A sink without `message-class` then looks up the latest schema under that subject, exactly as it would under `<topic>-value`. See [Subject naming](../how-to/schema-evolution.md#subject-naming).

## Projection pushdown

The decoding format supports projection pushdown. When the connector pushes the query's projection down to its format, the source converts only the columns the query reads: for `SELECT a, b FROM wide_table`, the decoder builds converters for `a` and `b` only, and skips every other declared field of each Protobuf message, however wide, nested, repeated or map-typed it is. `EXPLAIN` shows the pushed projection on the scan, for example `TableSourceScan(table=[[default_catalog, default_database, wide_table, project=[a, b]]], fields=[a, b])`.

- **Nested fields, when the connector pushes them.** The format also accepts nested projections (its `supportsNestedProjection()` returns `true`): for `SELECT payload.a FROM wide_table`, a connector that pushes nested projections into its format hands the decoder the path `payload.a`, and the decoder walks each record's message to that field without converting the other fields of `payload`. `EXPLAIN` then shows the flattened column the planner generates for the path, for example `project=[payload_a]`. The path is matched to the message by the declared column names along it (a column named after a `oneof` reads that oneof, whose columns are its cases by position), the same way a full read matches it, and it reads NULL where a full read of the `ROW` column would: when a message along the path is unset, when a oneof case along it is not the one set, or when the writer's message has no field for it. A message column declared with a [declared alternative](type-mapping.md#declared-alternatives) such as `STRING` or `BYTES` is not a `ROW`, so a path ends there and reads the whole message.
- **Top-level columns only with the `filesystem` and Kafka connectors.** Neither connector pushes nested projections (the `filesystem` connector's `supportsNestedProjection()` returns `false` in Flink 1.20 and 2.2), so with them a query that reads part of a nested `ROW` column (`SELECT f.x FROM wide_table`) pushes down the whole column `f`, and `f` is converted in full. Declaring the `ROW` column with only the fields the job reads (`f ROW<x STRING>`) keeps that cost down, as fields a `ROW` does not declare are never converted.
- **The connector decides.** Only a connector that pushes projections into its format benefits, such as the `filesystem` connector. The Kafka connector (`3.4.0-1.20` and `5.0.0-2.2`) does not push projections into its sources, so a Kafka table still converts every value column it declares; declare only the columns the job needs to keep that cost down.
- **Kafka tables: declare only what the job reads.** Until the Kafka connector supports projection pushdown (tracked upstream as [FLINK-32609](https://issues.apache.org/jira/browse/FLINK-32609), part of [FLINK-32607](https://issues.apache.org/jira/browse/FLINK-32607)), the way to skip unread fields on a Kafka table is to leave them out of its DDL: a column the table does not declare, and a field a `ROW` column does not declare, is never converted. Declare one table per access pattern if jobs read different subsets of the same topic. For example, a job that only needs the order ID and one field of a wide `payload` message can read the topic through:

  ```sql
  CREATE TABLE orders_slim (
    id STRING,
    payload ROW<a BIGINT>
  ) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'proto-confluent',
    'value.proto-confluent.url' = 'http://localhost:8081'
  );
  ```

  The message's other fields are still parsed (the Protobuf payload is decoded in full before any column is read), but none of them is converted into the row. With `message-class`, the check covers exactly the declared columns, so a slim table still fails fast on a column that does not match the message.
- **`message-class` checks follow the projection.** With pushdown, the source decoder is built for the projected columns, so the [message-class column check](../how-to/message-class.md#read-with-a-message-class) covers those columns in a streaming query. In batch mode the planner also builds the decoder for the whole table while it validates the source, so every column is still checked there.
