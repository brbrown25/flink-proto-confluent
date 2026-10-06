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

Full rules: [Schema evolution and subject naming](../reference/configuration.md#schema-evolution-and-subject-naming).
