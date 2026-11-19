# Generate table DDL from a registry subject

Writing the column list of a `CREATE TABLE` by hand means matching the Protobuf schema field by field, including nested `ROW`, `ARRAY` and `MAP` types and the nullability rules, and a mistyped column name silently reads as `NULL`. The `-all` JAR is also a command line tool whose `ddl` command reads a schema from the Schema Registry (or a compiled message class) and prints the statement for you.

## Generate a `CREATE TABLE` statement

Run the `-all` JAR for your Flink line with `java -jar`. It needs Java 17 or newer and nothing else on the classpath: no Flink installation, no extra JARs.

=== "Flink 1.20"

    ```bash
    java -jar flink-proto-confluent-<version>-all.jar ddl \
      --url http://schema-registry:8081 \
      --subject orders-value \
      --bootstrap-servers kafka:9092 \
      --option scan.startup.mode=earliest-offset
    ```

=== "Flink 2.x"

    ```bash
    java -jar flink-proto-confluent-flink2-<version>-all.jar ddl \
      --url http://schema-registry:8081 \
      --subject orders-value \
      --bootstrap-servers kafka:9092 \
      --option scan.startup.mode=earliest-offset
    ```

For an `orders-value` subject whose `Order` message has a nested message, a repeated message, a map and a `Money` type imported from another registered schema, it prints:

```sql
CREATE TABLE `orders` (
  `order_id` STRING,
  `customer` ROW<`name` STRING, `email` STRING>,
  `items` ARRAY<ROW<`sku` STRING, `quantity` INT, `price` ROW<`currency` STRING, `units` BIGINT>> NOT NULL> NOT NULL,
  `attributes` MAP<STRING, STRING> NOT NULL,
  `total` ROW<`currency` STRING, `units` BIGINT>,
  `tags` ARRAY<STRING> NOT NULL
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'value.format' = 'proto-confluent',
  'value.proto-confluent.url' = 'http://schema-registry:8081',
  'value.proto-confluent.topic' = 'orders',
  'scan.startup.mode' = 'earliest-offset'
);
```

Paste it into the SQL client, or redirect it to a file and run that with `sql-client.sh -f`. The statement goes to standard output and notes go to standard error, so `> orders.sql` captures only the SQL.

By default the command:

- reads the **latest** version of the subject and resolves its references (imported schemas registered under other subjects);
- uses the **first message** in the schema, the one Confluent serializers write unless told otherwise;
- takes the topic, and the table name, from a `<topic>-value` subject (`orders-value` gives `orders`);
- maps types and nullability exactly as the format does: `message` fields become `ROW`s, `repeated` fields `ARRAY`s, `map` fields `MAP`s, enums `STRING`, `google.protobuf.Timestamp` `TIMESTAMP_LTZ(9)`, the wrapper types their nullable scalar and `google.protobuf.Struct`, `Value` and `ListValue` a JSON `STRING`. The columns are the same as those `ProtoToTableDDL` returns in Java. [Type mapping](../reference/type-mapping.md) lists every mapping, and the column types you can declare instead.

The `'properties.bootstrap.servers'` option is only written when you pass `--bootstrap-servers`; the Kafka connector needs it to read. Add any other table option with `--option key=value` (repeatable): startup mode, group ID, bounded mode. An `--option` with the same key as a generated option replaces it, e.g. `--option connector=upsert-kafka`.

## Print only the columns

`--columns-only` prints the column list, one column per line, for pasting into a statement you write yourself:

```bash
java -jar flink-proto-confluent-<version>-all.jar ddl \
  --url http://schema-registry:8081 --subject orders-value --columns-only
```

## Pick a version, a message, a table name or a topic

| Flag | Effect |
| --- | --- |
| `--version <n>` | Read version `n` of the subject instead of the latest (`--version latest` is the default). |
| `--message-name <name>` | Use another message in the schema, by fully-qualified name, e.g. `com.example.OrderKey`. |
| `--table <name>` | Table name. It is always quoted as a single identifier. Default: the topic. |
| `--topic <topic>` | Kafka topic, written to `'topic'` and `'value.proto-confluent.topic'`. Required when the subject is not `<topic>-value`. |
| `--table-registry-url <url>` | Registry URL written into the statement, when Flink reaches the registry at another address than the machine running the command (e.g. `localhost:8081` from your laptop, `schema-registry:8081` inside Docker). Default: `--url`. |

## Nullability

The generated columns follow the format's rules: Protobuf `message` fields are nullable `ROW`s, repeated fields and maps are `NOT NULL`, and scalars are nullable. `--strict-not-null` makes scalar columns `NOT NULL`, the way hand-written DDL often marks them; `TIMESTAMP_LTZ`, `DATE` and `TIME` columns stay nullable and `ROW`s keep their nullability. With it, `--keep-nullable` keeps named fields nullable. Paths are dot-separated field names, array elements add `[]` and map keys and values add `.key` and `.value`:

```bash
java -jar flink-proto-confluent-<version>-all.jar ddl \
  --url http://schema-registry:8081 --subject orders-value \
  --strict-not-null --keep-nullable customer.email,items[].sku
```

These are the `strictNotNull` and `keepNullablePaths` arguments of `ProtoToTableDDL` in Java.

## Recursive messages

A message that contains itself (`message Tree { Tree child = 1; }`, directly or through other messages) has no finite `ROW` type, so by default the command fails on it with exit status `1` and `error: Cyclic schemas are not supported.`, naming the message and the field that closes the cycle. `--recursive-message` derives a column for that field instead:

| Flag | The field becomes |
| --- | --- |
| `--recursive-message fail` | Nothing: the command fails (the default). |
| `--recursive-message bytes` | `VARBINARY`, the message's Protobuf encoding, every level included. |
| `--recursive-message json` | `STRING`, the message's JSON, every level included. |
| `--recursive-message row:<n>` | A `ROW` unrolled `n` times; deeper levels are left out. |

```bash
java -jar flink-proto-confluent-<version>-all.jar ddl \
  --url http://schema-registry:8081 --subject trees-value \
  --recursive-message row:2 --columns-only
```

For `message Tree { string name = 1; Tree child = 2; }` this prints:

```sql
`name` STRING,
`child` ROW<`name` STRING, `child` ROW<`name` STRING>>
```

The table reads and writes every column the statement declares. [Recursive messages](../reference/type-mapping.md#recursive-messages) in the type mapping describes each mode, including `oneof` and `map` fields. This is the `RecursionStrategy` argument of `ProtoToTableDDL` in Java, and the catalog's `recursive-message` option.

## From a compiled message class

Instead of a subject, name a generated message class with `--message-class` and point `--classpath` at the JAR or class directory that holds it (separate several entries with `:`, or `;` on Windows). Nested generated classes use `$`:

```bash
java -jar flink-proto-confluent-<version>-all.jar ddl \
  --message-class 'com.example.OrderProto$Order' \
  --classpath build/libs/my-protos.jar \
  --url http://schema-registry:8081 --topic orders
```

A full statement needs `--topic` (there is no subject to derive it from) and a registry URL for the format; `--columns-only` needs neither. The class is only read to build the columns: add `--option value.proto-confluent.message-class=com.example.OrderProto\$Order` if the table should also [serialize with it](message-class.md).

## Registry authentication and TLS

The command takes the same connection options as the format, with the same names, and sends them to the registry:

| Flag | Format option |
| --- | --- |
| `--ssl.keystore.location`, `--ssl.keystore.password` | `ssl.keystore.location`, `ssl.keystore.password` |
| `--ssl.truststore.location`, `--ssl.truststore.password` | `ssl.truststore.location`, `ssl.truststore.password` |
| `--basic-auth.credentials-source`, `--basic-auth.user-info` | `basic-auth.credentials-source`, `basic-auth.user-info` |
| `--bearer-auth.credentials-source`, `--bearer-auth.token` | `bearer-auth.credentials-source`, `bearer-auth.token` |
| `--property <key>=<value>` (repeatable) | `properties.<key>` |

Each one is also written into the statement as `'value.proto-confluent.<option>'`, so the table connects the same way, **except credentials**: passwords, `basic-auth.user-info`, `bearer-auth.token` and `--property` keys that look like credentials (containing `password`, `user.info`, `token`, `secret` or `jaas`) are left out, and a note on standard error names them. Add them to the statement yourself, or pass `--include-secrets` to have them written in. See [SSL and authentication](ssl-and-auth.md) for what each option does.

## Exit status

| Status | Meaning |
| --- | --- |
| `0` | The DDL was printed. |
| `1` | The schema could not be resolved: the subject, version, message or class does not exist, the schema is not Protobuf, or the registry could not be reached. The error is on standard error. |
| `2` | Invalid arguments. The error and the usage are on standard error. |

`java -jar flink-proto-confluent-<version>-all.jar ddl --help` lists every flag.

## How the JAR runs without Flink

The `-all` JAR is meant for Flink's `lib/`, so it bundles neither Flink nor SLF4J (see [Classpath and dependency conflicts](../reference/classpath.md)). The `ddl` command uses no Flink classes: it maps the schema with the same code the format uses, in a form that does not depend on Flink. The Schema Registry client does need the SLF4J API, so the JAR carries a copy of `slf4j-api` as a nested JAR that no class loader sees while the JAR sits in `lib/`; the command loads it into a private class loader only when you run it with `java -jar` and SLF4J is not otherwise on the classpath. Nothing is logged.

## From Java

The same resolution is available as an API, for example in a build step or a catalog:

```java
SchemaRegistrySchemaResolver resolver =
    SchemaRegistrySchemaResolver.create("http://schema-registry:8081", Map.of());
ResolvedProtoSchema schema = resolver.resolve("orders-value"); // or resolve(subject, version, messageName)
String columns = String.join(", ", schema.toSqlType().toColumnDefinitions());
LogicalType rowType = ProtoToLogicalType.toLogicalType(schema.getDescriptor()); // inside Flink
```

`SchemaRegistryClients.clientProperties(...)` turns format options such as `basic-auth.user-info` into registry client properties.
