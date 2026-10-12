# Schema evolution and subjects

A source decodes each record with the **writer schema** named by the schema ID in the record's Confluent wire header, so a topic holding several schema versions is readable without extra configuration.

- **Added columns:** a table column missing from the writer's schema reads as `NULL` (logged once per converter at `WARN`). A table declared against v2 still reads v1 records. The same warning is how a mistyped column name surfaces: it is an always-`NULL` column.
- **Removed columns:** declare only the columns you need.
- **`use-schema-id`:** pin the schema ID stamped on a sink. The schema must already be registered under the subject.
- **`auto-register-schemas`:** with `false` (default) writing to a subject without a schema fails. With `true`, a change the registry rejects under its compatibility level fails the job with the registry's error.
- **Subject naming:** no dedicated option. Use the `properties` passthrough:

```sql
'value.proto-confluent.properties' = 'value.subject.name.strategy:io.confluent.kafka.serializers.subject.RecordNameStrategy'
```

Record-based strategies need `message-class` or a row-derived schema, since no subject can be named before a schema exists.

## Lifecycle: initial, compatible, breaking

The registry's compatibility mode decides which schema changes can become a new version. With the default `BACKWARD` mode:

1. **v1** registers with the first record.
2. **v2 adds a field** (`string currency = 4`). Compatible, so it registers. A table declared against v2 reads v1 and v2 records; the v1 rows get `NULL` for `currency`.
3. **v3 retypes a field** (`double amount = 3` becomes `string amount = 3`). The registry answers HTTP 409 (`FIELD_SCALAR_KIND_CHANGED`) and the subject stays at two versions. A Flink sink with `auto-register-schemas = true` surfaces that error and fails the job.

See it in the [schema evolution demo](../demos.md#schema-evolution-compatible-change-then-a-breaking-one).

## Compatibility modes

| Change against the latest version | `BACKWARD` | `FORWARD` | `FULL` | `NONE` |
| --- | --- | --- | --- | --- |
| Add a field | accepted | accepted | accepted | accepted |
| Remove a field | accepted | accepted | accepted | accepted |
| Change a field's type | rejected | rejected | rejected | accepted |

Confluent's Protobuf checker is lenient for additions, removals and renames, so the modes differ mostly on type changes. The `*_TRANSITIVE` variants compare against **every** earlier version instead of only the latest: a schema that only adds a field to a (forced-through) breaking v3 is accepted under `BACKWARD` but rejected under `BACKWARD_TRANSITIVE`, because v1 had the old type.

!!! note "`/compatibility/.../versions/latest` is not transitive"
    The registry's compatibility *check* endpoint compares against the latest version only, even for a `*_TRANSITIVE` subject. Registering the schema is what applies the transitive rule, so use the registration result (or check against a specific older version) when you need the transitive verdict.

See the [compatibility modes demo](../demos.md#compatibility-modes).

## When a breaking change gets through

Compatibility `NONE`, a subject-level override, or a producer that never checked can put an incompatible schema on a topic. A reader decodes each record with its writer schema and then maps it to the table's column types, so a table still declared with the old types cannot decode the new records (for example `Unsupported protobuf type: STRING for a SQL type: DOUBLE`).

- **Default (`on-deserialize-error = fail`):** the job fails on the first such record.
- **`skip` with a [`dead-letter-topic`](dead-letter-topic.md):** the job keeps running, the incompatible records are dropped from the result, and their original bytes (with the new schema's ID in the wire header) are kept on the DLQ with `error.class`, `error.message` and `source.topic` headers. Fix the table's column types (or the producer) and replay the DLQ.

See the [breaking change demo](../demos.md#breaking-change-and-the-dead-letter-topic).

!!! warning "A sink without `message-class` follows the registry"
    When the subject already has a schema and no `message-class` is set, a sink serializes against the latest registered schema rather than deriving a new one from the table. Adding a column to the table does not register a new version; register the new schema first, or pin the class with [`message-class`](message-class.md).

Full rules: [Schema evolution and subject naming](../reference/configuration.md#schema-evolution-and-subject-naming).
