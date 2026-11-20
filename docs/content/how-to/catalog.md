# Query topics through a Schema Registry catalog

Even with [generated DDL](generate-ddl.md), every topic needs a `CREATE TABLE`, and the statement goes stale when the schema evolves. The `proto-confluent` catalog makes every Kafka topic with a registered protobuf schema a table, with its columns read from the Schema Registry each time a query is planned.

```sql
CREATE CATALOG sr WITH (
  'type' = 'proto-confluent',
  'url' = 'http://schema-registry:8081',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-sql',
  'scan.startup.mode' = 'earliest-offset'
);

SHOW TABLES FROM sr.`default`;
SELECT * FROM sr.`default`.orders;
```

The catalog ships in the same JAR as the format (both the library JAR and the `-all` JAR) and is registered with Flink's service loader, so nothing beyond the JARs you already use for the format is needed: the Kafka SQL connector and this project's JAR in `lib/` (or on the job's classpath).

## Which topics become tables

The catalog reads subjects named with the **`TopicNameStrategy`**, Confluent's default:

- A subject `<topic>-value` whose **latest** schema is protobuf becomes table `<topic>`. `orders-value` gives `orders`.
- Subjects holding Avro or JSON Schema schemas are skipped, as are subjects that do not end in `-value` (for example schemas registered only to be imported by others).
- If `<topic>-key` also holds a protobuf schema, the key message's fields become columns too, prefixed with `key_` and listed before the value columns. A key subject of another type, or no key subject, gives a table with value columns only.
- The table reads the **first message** in the schema, the one Confluent serializers write unless told otherwise.

Column types and nullability are mapped exactly as the format maps them; the columns are the same ones the [`ddl` command](generate-ddl.md) prints for the subject.

All tables are in one database, `default` (change its name with `default-database`). Table names that are not plain identifiers need backquotes: ``SELECT * FROM sr.`default`.`orders-v2` ``.

For an `orders` topic with an `orders-value` and an `orders-key` subject, the table the catalog returns is the same as this statement (see it with `SHOW CREATE TABLE sr.`default`.orders`):

```sql
CREATE TABLE orders (
  `key_order_id` STRING,
  `order_id` STRING,
  `customer` ROW<`name` STRING, `email` STRING>,
  -- ... the rest of the value columns
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'key.format' = 'proto-confluent',
  'key.proto-confluent.url' = 'http://schema-registry:8081',
  'key.proto-confluent.topic' = 'orders',
  'key.proto-confluent.use-schema-id' = '<ID of the latest orders-key schema>',
  'key.proto-confluent.is_key' = 'true',
  'key.fields' = 'key_order_id',
  'key.fields-prefix' = 'key_',
  'value.fields-include' = 'EXCEPT_KEY',
  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'value.proto-confluent.use-schema-id' = '<ID of the latest orders-value schema>',
  -- plus the catalog's properties.*, scan.* and sink.* options
);
```

## Catalog options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `type` | yes | | `proto-confluent`. |
| `url` | yes | | Schema Registry URL, or several separated by commas. |
| `default-database` | no | `default` | Name of the catalog's only database. |
| `key.enabled` | no | `true` | Whether a protobuf `<topic>-key` subject adds key columns. |
| `key.fields-prefix` | no | `key_` | Prefix of the key columns, so a key field and a value field of the same name do not clash. The table fails to load if a prefixed key column still has a value column's name; pick another prefix or set `key.enabled` to `false`. |
| `recursive-message` | no | `fail` | How a table maps the field that makes a [recursive message](../reference/type-mapping.md#recursive-messages) contain itself: `fail` (the table fails to load, naming the message, the field and this option), `bytes` (`BYTES`, the serialized message), `json` (`STRING`, its JSON) or `row:<n>` (a `ROW` unrolled `n` times; deeper levels are left out). Applies to key and value columns. |
| `ssl.keystore.location`, `ssl.keystore.password`, `ssl.truststore.location`, `ssl.truststore.password` | no | | TLS to the Schema Registry, as in the [format](ssl-and-auth.md). |
| `basic-auth.credentials-source`, `basic-auth.user-info` | no | | HTTP Basic authentication to the Schema Registry, as in the format. |
| `bearer-auth.credentials-source`, `bearer-auth.token` | no | | Bearer-token authentication to the Schema Registry, as in the format. |
| `schema-registry.properties.<key>` | no | | Any other Schema Registry client property, e.g. `schema-registry.properties.max.retries`. The format's equivalent is `properties.<key>`; the catalog needs the longer prefix because `properties.<key>` belongs to Kafka. |
| `properties.<key>` | no | | Kafka client properties (`properties.bootstrap.servers`, `properties.group.id`, `properties.security.protocol`, ...), set on every table. |
| `scan.<option>`, `sink.<option>` | no | | Kafka connector source and sink options (`scan.startup.mode`, `scan.bounded.mode`, `sink.delivery-guarantee`, ...), set on every table. |
| `table.<option>` | no | | Any other table option, set on every table with the `table.` prefix removed, e.g. `'table.value.proto-confluent.on-deserialize-error' = 'skip'`. Applied last, so it replaces a generated option of the same name. |

The connection options are passed to the format of every table (as `value.proto-confluent.<option>` and `key.proto-confluent.<option>`), so the tables reach the registry the same way the catalog does. Any other option is rejected when the catalog is created.

!!! warning "Catalog tables carry the registry credentials"
    Because the connection options are copied into every table, the credentials among them (`basic-auth.user-info`, `bearer-auth.token`, `ssl.keystore.password` and `ssl.truststore.password`, and any user info in `url`) are part of each table's options. Anyone who can use the catalog can read them, for example with `SHOW CREATE TABLE sr.`default`.orders`. This is how Flink table options work, and it is the same for a hand-written table. Give the catalog credentials that only read the registry, and only register the catalog in sessions whose users may see them. See [Where credentials are visible](ssl-and-auth.md#where-credentials-are-visible).

## Override options per query

Every table option, including those the catalog sets, can be overridden for one query with a [dynamic table options](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/hints/#dynamic-table-options) hint:

```sql
SELECT order_id
FROM sr.`default`.orders /*+ OPTIONS(
  'scan.startup.mode' = 'specific-offsets',
  'scan.startup.specific-offsets' = 'partition:0,offset:42',
  'properties.group.id' = 'adhoc'
) */;
```

Hints are on by default (`table.dynamic-table-options.enabled`).

## Writing

`INSERT INTO` a catalog table works. The sink writes with the schema IDs the table was planned with (`use-schema-id`), the same schemas its columns came from, and registers nothing. The key columns go to the record key in the `<topic>-key` schema.

```sql
INSERT INTO sr.`default`.orders
SELECT 'o-3', 'o-3', customer, items, attributes, total, tags
FROM sr.`default`.orders WHERE order_id = 'o-1';
```

## Limitations

- **`TopicNameStrategy` only.** Topics whose subjects are named with `RecordNameStrategy` or `TopicRecordNameStrategy` are not listed. Declare those tables with `CREATE TABLE` in another catalog.
- **Read-only.** `CREATE`, `ALTER` and `DROP` of databases, tables, partitions and functions are rejected with `Catalog '<name>' is a read-only view of the Schema Registry`. To add a table, register a protobuf schema under `<topic>-value`. There are no views, functions or partitions, and statistics are unknown.
- **No watermarks, metadata columns or primary keys.** To add a watermark or Kafka metadata columns (`timestamp`, `partition`, `offset`), use `CREATE TABLE ... LIKE sr.`default`.orders` in another catalog, or the [`ddl` command](generate-ddl.md).
- **One message per subject.** The first message in the schema is used.
- **Recursive messages need `recursive-message`.** A topic whose message contains itself fails to load as a table until the catalog sets `recursive-message` to `bytes`, `json` or `row:<n>`; the same setting applies to every table of the catalog. Create a second catalog to read another topic with another setting.
- **Schema changes are picked up when a query is planned.** A running job keeps the columns and the write schema ID it was planned with.
- **`SHOW TABLES` asks the registry about every `-value` subject.** On a registry with many subjects, listing tables takes one request per subject.
- **Credentials appear in table options.** Schema Registry credentials given to the catalog are set on each table so its format can connect, so `SHOW CREATE TABLE` prints them, as it would for a hand-written table. Flink masks options whose names contain `password`, `token`, `basic-auth` and similar in its error messages and logs.

## Design

This section records the decisions made for [issue #169](https://github.com/brbrown25/flink-proto-confluent/issues/169).

**Scope.** Only `TopicNameStrategy` subjects in v1: it is the default, it maps a subject to exactly one topic, and it lets the catalog find the key schema (`<topic>-key`) without guessing. Other strategies name subjects after records, so a subject does not say which topic it belongs to.

**Keys.** A protobuf `<topic>-key` subject adds the key message's fields as columns, read and written through the Kafka connector's `key.format`, `key.fields`, `key.fields-prefix` and `value.fields-include = 'EXCEPT_KEY'`. The prefix avoids the common case of a key field (`order_id`) repeating a value field. A clash that the prefix does not resolve fails the table with a message naming the field, rather than silently dropping key columns.

**Table options.** The catalog is configured once and its Kafka options (`properties.*`, `scan.*`, `sink.*`) are copied to every table; `table.<option>` is an escape hatch for anything else, including format options. The issue's question of how defaults are overridden is answered by Flink's `OPTIONS` hints, which apply to catalog tables like any other. The format's `properties.*` prefix for Schema Registry client properties would collide with Kafka's, so the catalog uses `schema-registry.properties.*` for those.

**Writing.** Rather than rejecting `INSERT INTO`, which a catalog cannot do on its own (the Kafka connector creates the sink), the catalog makes writes correct: each table pins `use-schema-id` to the schema version its columns came from. Without it the sink would write with whichever version is the subject's latest when it first writes, which need not be the version the table was planned against if one is registered in between. Before [issue #210](https://github.com/brbrown25/flink-proto-confluent/issues/210) was fixed, a sink without the pin also looked the schema it rebuilt from the descriptor up by content, and that lookup did not find registered schemas whose text differs from the rebuilt one (a `map` field, for example), failing with `Schema not found; error code: 40403`. A sink now writes under the ID it read from the registry, so the pin is no longer needed for that, but it is kept for the first reason.

**Packaging.** Same artifact as the format, with no dependency on the Kafka connector: its option names are written as strings. The catalog factory shares the `proto-confluent` identifier with the format factory. Flink looks factories up by kind (catalog, format, connector) before identifier, so the two do not conflict; a test checks both are found.

**Flink 1.20 and 2.x.** One source tree builds for both lines. The catalog uses only API present in both and in `flink-table-common`: `AbstractCatalog`, `CatalogTable.newBuilder()` and `org.apache.flink.table.api.Schema` (not the `TableSchema` API that 2.x removed).

**Schema refresh.** `getTable` resolves the latest version on every call, so each query is planned against the current schema; the registry client caches schemas by ID. `listTables` lists subjects on every call, without a catalog-level cache.

**Reused code.** Schema resolution, the registry client and the connection option mapping come from the same `schema` package the [`ddl` command](generate-ddl.md) uses (`SchemaRegistrySchemaResolver`, `SchemaRegistryClients`), and the types from `ProtoToLogicalType`, so the catalog, the `ddl` command and the format agree on columns.
