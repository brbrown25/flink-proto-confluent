# Schema evolution and subjects

A source decodes each record with the **writer schema** named by the schema ID in the record's Confluent wire header, so a topic holding several schema versions is readable without extra configuration.

- **Added columns:** a table column missing from the writer's schema reads as `NULL` (logged once per converter at `WARN`). A table declared against v2 still reads v1 records. The same warning is how a mistyped column name surfaces: it is an always-`NULL` column.
- **Sink columns missing from the registered schema:** with no `message-class`, a sink serializes against the latest registered schema and does not register a new version for an extra column. The job fails fast with `Column(s) 'c' not found in target Protobuf schema '<message>'. ... Register a schema that includes them, or pin 'message-class'.` listing every missing column. Register the new schema version first, or pin `message-class`.
- **Removed columns:** declare only the columns you need.
- **`use-schema-id`:** pin the schema ID stamped on a sink. The schema must already be registered under the subject.
- **`auto-register-schemas`:** with `false` (default) writing to a subject without a schema fails. With `true`, a change the registry rejects under its compatibility level fails the job with the registry's error.
- **Registry errors and schema caching on a sink:** only a "subject not found" answer makes a sink derive a schema from the table; any other registry error fails the write. The schema a sink resolves is kept until restart unless `schema-cache.ttl` is set. See [How a sink picks its schema](#how-a-sink-picks-its-schema-and-how-long-it-keeps-it).
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

!!! warning "The compatibility check is not transitive"
    `POST /compatibility/subjects/{subject}/versions/latest` compares the schema against the **latest version only**, even when the subject is in `BACKWARD_TRANSITIVE`, `FORWARD_TRANSITIVE` or `FULL_TRANSITIVE`. Only **registration** enforces the transitive rule. A check can therefore answer `is_compatible: true` for a schema the registry then rejects with HTTP 409 on registration, so a CI "dry run" through that endpoint gives a false pass.

    To dry-run a transitive mode, do one of:

    - Check against **each prior version** explicitly with `POST /compatibility/subjects/{subject}/versions/{n}` for every registered `n` (list them with `GET /subjects/{subject}/versions`) and require all to pass. The [demo](../demos.md#compatibility-modes) helper does this: `SCHEMA=... check --all-versions SUBJECT`.
    - Attempt the registration in a **scratch subject** seeded with the same history and the same compatibility mode, then delete the scratch subject.

See the [compatibility modes demo](../demos.md#compatibility-modes).

## When a breaking change gets through

Compatibility `NONE`, a subject-level override, or a producer that never checked can put an incompatible schema on a topic. A reader decodes each record with its writer schema and then maps it to the table's column types, so a table still declared with the old types cannot decode the new records (for example `Unsupported protobuf type: STRING for a SQL type: DOUBLE`).

- **Default (`on-deserialize-error = fail`):** the job fails on the first such record.
- **`skip` with a [`dead-letter-topic`](dead-letter-topic.md):** the job keeps running, the incompatible records are dropped from the result, and their original bytes (with the new schema's ID in the wire header) are kept on the DLQ with `error.class`, `error.message` and `source.topic` headers. Fix the table's column types (or the producer) and replay the DLQ.

See the [breaking change demo](../demos.md#breaking-change-and-the-dead-letter-topic).

!!! warning "A sink without `message-class` follows the registry"
    When the subject already has a schema and no `message-class` is set, a sink serializes against the latest registered schema rather than deriving a new one from the table. Adding a column to the table does not register a new version; register the new schema first, or pin the class with [`message-class`](message-class.md).

## How a sink picks its schema, and how long it keeps it

A sink without `message-class` asks the registry for the latest schema of its subject (`GET /subjects/{subject}/versions/latest`) and serializes with it. A sink with `message-class`, or with a record-based subject-naming strategy, never makes that lookup, so nothing in this section applies to it.

### What each registry answer does

| Answer to the lookup | What the sink does |
| --- | --- |
| A schema | Serializes with it. Nothing is registered under the subject. |
| Subject not found (HTTP 404, error code `40401`) | Derives a schema from the table columns. With `auto-register-schemas = true` that schema is registered as the subject's first version; with `false` the write fails because the subject has no schema. |
| Anything else: `401`, `403`, `429`, `5xx`, a timeout, a refused or reset connection, an unknown host | The write fails with the registry's error as the cause. Nothing is registered. |
| Version not found (HTTP 404, error code `40402`) | Fails, like any other error. The registry is saying the subject exists, so the sink does not treat it as missing. |
| A 404 that does not carry error code `40401` (an HTML page from a proxy or ingress, a wrong path prefix in `url`) | Fails, like any other error. It is not the registry saying the subject is missing. |
| A schema that is not Protobuf, or cannot be parsed | Fails. |

A subject whose versions were all soft-deleted also answers "subject not found" (checked against `cp-schema-registry` 7.5.0), so the sink treats it as missing: with `auto-register-schemas = true` it registers the table-derived schema under it.

Only "subject not found" leads to the table-derived schema. Before this rule every failed lookup did, so with `auto-register-schemas = true` a registry outage, an expired credential or a timeout at the wrong moment could register a table-derived version on top of a hand-authored schema. Such a job now fails instead; fix the cause and restart it. The Confluent client retries `5xx` answers itself (three more attempts by default, tunable through `properties` with `max.retries` and `retries.wait.ms`) before the sink sees the failure.

The failure is an unchecked `org.apache.kafka.common.errors.SerializationException` whose message names the subject, the HTTP status and the registry error code, and whose cause is the registry client's exception. It fails the task, and Flink's restart strategy decides what happens next. A failed lookup is not remembered: the next record, or the restarted task, asks again.

### How long the schema is kept: `schema-cache.ttl`

By default the sink keeps the schema it resolved at its first write for as long as the task runs. A version registered later is not picked up until the job (or the task) restarts.

Set `schema-cache.ttl` to have a running sink pick up new versions:

```sql
'value.proto-confluent.schema-cache.ttl' = '5 min'
```

The value is a Flink duration (`30 s`, `5 min`, `1 h`) and must be positive. Values under one second are accepted but gain nothing, because the client cache described below is never shorter than one second. Once the schema is older than the TTL, the next record triggers a new lookup:

- **Same schema ID and version:** nothing changes, and the schema is good for another TTL.
- **A new version:** the sink switches to it. Records from then on carry the new schema ID, and the row converter is rebuilt for the new descriptor, so a column that only the new version has starts being written. If the table has a column the new version lacks, the job fails with the `Column(s) ... not found in target Protobuf schema` error described above, exactly as it would after a restart. This is why the TTL is opt-in: with it, registering an incompatible version can fail a job that is running.
- **Subject not found:** the subject was deleted while the sink was running. The sink does not go back to the "subject not found" row of the table above: it keeps the schema it already resolved, exactly as for any other failure below, and never switches to a table-derived schema in place of one it read from the registry. What happens to the write itself is up to the Confluent serializer, which needs the schema's ID under the subject: with `auto-register-schemas = true` it may register that same registry schema under the subject again, as any auto-registering producer would; with `false` the write fails once the ID can no longer be resolved. If the subject is re-registered with a new schema, the next successful refresh picks that up. After a restart the sink starts from scratch, so a subject that is still missing then is treated as missing.
- **Any other failure** (including `401`/`403`, a schema that is no longer Protobuf, or one that cannot be parsed): the sink logs a `WARN` and keeps serializing with the schema it already has, then retries the lookup after the TTL or 30 seconds, whichever is shorter, counted from the moment the failed lookup returned. A registry outage therefore does not stop a sink that has already resolved its schema; it only delays picking up a new version. The retry interval keeps an unreachable registry from being called once per record, though each retry can still block one record for as long as the client's timeout. A failure that will not heal on its own, such as revoked credentials, is only visible as that repeating `WARN` until the task restarts, at which point the lookup fails the job; watch the log for it.

The Schema Registry client has a cache of its own for "latest version" answers (`latest.cache.ttl.sec`, 60 seconds by default). When `schema-cache.ttl` is set, the sink caps that client cache at the TTL (between 1 and 60 seconds) unless you set `latest.cache.ttl.sec` yourself through `properties`. A new version is therefore picked up at most two TTLs after it is registered, or the TTL plus 60 seconds for TTLs above a minute.

`schema-cache.ttl` applies to sinks only. A source reads each record with the schema ID in its wire header and needs no refresh.

Full rules: [Schema evolution and subject naming](../reference/configuration.md#schema-evolution-and-subject-naming).
