# Type mapping

How the format maps Protobuf types to Flink SQL types, in both directions. The source reads each column from the Protobuf field of the same name, and the sink writes each column to it. The column's type, as the table declares it, decides how the value is converted: a table can declare the type the format derives (below) or, for some fields, one of the alternatives further down.

## Derived types

These are the types the [`ddl` command](../how-to/generate-ddl.md), the [catalog](../how-to/catalog.md) and `ProtoToTableDDL` derive for a Protobuf schema. A sink that registers a schema of its own derives it the other way round: `BIGINT` becomes `int64`, a `ROW` a nested message, `TIMESTAMP_LTZ` a `google.protobuf.Timestamp`, and so on.

| Protobuf type | Flink SQL type |
| --- | --- |
| `int32`, `sint32`, `sfixed32` | `INT` (`TINYINT` / `SMALLINT` with the Confluent `connect.type` = `int8` / `int16` annotation) |
| `uint32`, `fixed32`, `int64`, `uint64`, `sint64`, `fixed64`, `sfixed64` | `BIGINT` |
| `float` | `FLOAT` |
| `double` | `DOUBLE` |
| `bool` | `BOOLEAN` |
| `string` | `STRING` |
| `bytes` | `BYTES` |
| `enum` | `STRING` (the value's name) |
| `confluent.type.Decimal` | `DECIMAL(p, s)` from the field's `precision` / `scale` annotation, `DECIMAL(10, 0)` without one |
| `google.type.Date` | `DATE` |
| `google.type.TimeOfDay` | `TIME(3)` |
| `google.protobuf.Timestamp` | `TIMESTAMP_LTZ(9)` |
| `google.protobuf.DoubleValue` / `FloatValue` | `DOUBLE` / `FLOAT` |
| `google.protobuf.Int32Value` | `INT` |
| `google.protobuf.Int64Value` / `UInt64Value` / `UInt32Value` | `BIGINT` |
| `google.protobuf.BoolValue` / `StringValue` / `BytesValue` | `BOOLEAN` / `STRING` / `BYTES` |
| `google.protobuf.Struct` / `Value` / `ListValue` | `STRING`, holding the value's JSON (see [JSON strings](#json-strings)) |
| `google.protobuf.Any` | `ROW<type_url STRING, value BYTES>`, like any other message: the packed message stays serialized in `value` |
| `map<K, V>` | `MAP<K, V>` |
| `repeated T` | `ARRAY<T>` |
| any other `message` | `ROW<...>` of its fields |
| `oneof` | `ROW<...>` of its cases |
| a `message` field that makes its message contain itself (a recursive message) | Derivation fails, unless the `recursive-message` option asks for `BYTES`, a JSON `STRING` or a `ROW` unrolled to a fixed depth (see [Recursive messages](#recursive-messages)) |

Message-typed fields and wrapper types are nullable; `repeated` fields and maps are `NOT NULL` (an unset repeated field is an empty array). Scalars are nullable in the derived DDL; `--strict-not-null` on the `ddl` command makes them `NOT NULL`.

## Declared alternatives

A table can declare one of these types for a field instead of the derived one. The source reads the field into it, the sink writes it back to the same Protobuf value, and the `message-class` check accepts it. Derived DDL never produces them, except for the field that closes the cycle of a [recursive message](#recursive-messages) when you set `recursive-message`: declare the column yourself, or edit the generated DDL.

| Protobuf type | Declared Flink SQL type | Conversion |
| --- | --- | --- |
| `google.protobuf.Timestamp` | `TIMESTAMP(p)` | The instant's wall-clock time in UTC. Writing reads the column's wall-clock time as UTC. When the sink derives a schema, a `TIMESTAMP(p)` column becomes a `google.protobuf.Timestamp` field. |
| `uint64`, `fixed64`, `google.protobuf.UInt64Value` | `DECIMAL(p, s)`, normally `DECIMAL(20, 0)` | The unsigned value, from 0 to 18446744073709551615. `DECIMAL(20, 0)` holds every value; with a smaller precision a value that does not fit reads as `NULL`. Writing fails on a negative value, a fractional value or one above 18446744073709551615 instead of wrapping it. |
| any `message` except `google.protobuf.Any` and wrapper-style messages | `STRING` | The message's JSON (see [JSON strings](#json-strings)). |
| `enum` | `INT` | The value's number instead of its name. In an open (proto3) enum, a number the enum does not define (written by a newer schema) reads as that number; a closed (proto2) enum drops such a number when the record is parsed, so the field reads as unset. Writing a number the enum does not define keeps it in an open (proto3) enum and fails for a closed (proto2) enum, which cannot hold it. Works for `repeated` enums (`ARRAY<INT>`) and map values too. |
| any `message` except wrapper-style messages | `BYTES` | The message's Protobuf encoding, kept serialized. Writing fails on bytes that do not parse as the message. For `google.protobuf.Any` this is the whole `Any`, type URL included. |
| `google.protobuf.Duration` | `INTERVAL DAY TO SECOND` (any precision, e.g. `INTERVAL DAY(2) TO SECOND(3)`) | The duration in milliseconds, the interval's resolution in Flink: nanoseconds below a millisecond are dropped when reading (toward zero). Negative durations work. When the sink derives a schema, an `INTERVAL DAY TO SECOND` column becomes a `google.protobuf.Duration` field. Derived DDL keeps reading a `Duration` as `ROW<seconds BIGINT, nanos INT>`. |

## Flink to Protobuf: the schema a sink derives

A sink writes with the latest schema registered under its subject, or with the class named by [`message-class`](../how-to/message-class.md). Only when the Schema Registry answers that the subject does not exist (and no `message-class` is set) does it derive a schema from the table's row type, as below, and register it when `auto-register-schemas` is `true`. The derived message is `com.flink.proto.confluent.Row`, in a `proto3` file.

| Flink SQL type | Protobuf field |
| --- | --- |
| `BOOLEAN` | `bool` |
| `TINYINT` | `int32` with the Confluent `connect.type` = `int8` annotation |
| `SMALLINT` | `int32` with the Confluent `connect.type` = `int16` annotation |
| `INT` | `int32` |
| `BIGINT` | `int64` |
| `FLOAT` | `float` |
| `DOUBLE` | `double` |
| `CHAR`, `VARCHAR`, `STRING` | `string` |
| `BINARY`, `VARBINARY`, `BYTES` | `bytes` |
| `TIMESTAMP_LTZ(p)` | `google.protobuf.Timestamp` |
| `TIMESTAMP(p)` | `google.protobuf.Timestamp`, the wall-clock time read as UTC |
| `INTERVAL DAY TO SECOND` | `google.protobuf.Duration`, in milliseconds |
| `DATE` | `google.type.Date` |
| `TIME(p)` | `google.type.TimeOfDay` |
| `DECIMAL(p, s)` | `confluent.type.Decimal` with `precision` = `p` and `scale` = `s` annotations |
| `ROW<...>` | a nested message named `<field>_Row` |
| `ARRAY<T>` | `repeated T` |
| `MAP<K, V>` | `repeated` `<Field>Entry { K key = 1; V value = 2; }`, the shape `protoc` gives a `map<K, V>` |
| `MULTISET<T>` | as `MAP<T, INT>`: each element with its count |
| `TIMESTAMP WITH TIME ZONE`, `INTERVAL YEAR TO MONTH`, `RAW`, structured and distinct types | not supported: deriving the schema fails with `Unsupported to derive Schema for type` |

Fields are numbered 1, 2, … in column order. A nullable column becomes a proto3 `optional` field and a `NOT NULL` column a plain proto3 field (without `optional`, so it has no presence); `ARRAY` and `MAP` columns are `repeated` either way, and a map entry's `key` and `value` are `optional`. To control the schema exactly (field numbers, enums, `oneof`s, imports), register it yourself before the sink first writes, or pin a `message-class`.

## Nullability

Protobuf and SQL disagree about `NULL`: most Protobuf fields cannot be unset, only left at their default value. The format applies these rules.

**Derived DDL** (the `ddl` command, the catalog and `ProtoToTableDDL`):

| Protobuf field | Derived nullability |
| --- | --- |
| Scalar (`int32`, `string`, `bool`, `bytes`, `enum`, …), proto3 plain or `optional`, proto2 `optional` | nullable |
| Scalar with the proto2 `required` label | `NOT NULL` |
| Message, including wrapper types, `Timestamp`, `Date`, `TimeOfDay`, `Decimal`, `Struct` / `Value` / `ListValue` | nullable |
| `oneof` | nullable `ROW`, and so is each of its cases |
| `repeated` | `ARRAY<...> NOT NULL`; a message element type is `ROW<...> NOT NULL` |
| `map` | `MAP<...> NOT NULL` |
| The table's own message (the top-level row) | `NOT NULL` |

**`--strict-not-null`** on the [`ddl` command](../how-to/generate-ddl.md) (and the `strictNotNull` argument of `ProtoToTableDDL`) rewrites the derived types for hand-written DDL that declares most columns `NOT NULL`: every type becomes `NOT NULL` (wrapper types and `DECIMAL` included) except `TIMESTAMP_LTZ`, `DATE` and `TIME`, which stay nullable, and `ROW`s, which keep their nullability. `ARRAY` and `MAP` stay `NOT NULL`. `--keep-nullable <path>[,<path>]` keeps the named fields nullable; a path is dot-separated field names, with `[]` for an array's element and `.key` / `.value` for a map's key and value.

**Reading.** A field without presence (a proto3 scalar without `optional`, a `repeated` field or a map) is never `NULL`: an unset value reads as its default (`0`, `''`, `false`, the enum's default value, an empty array or map), because the wire format cannot tell it from a value set to that default. A field with presence (a message, a wrapper type, a proto3 `optional` or proto2 scalar, a `oneof` case) reads as `NULL` when unset. A `oneof` column is `NULL` when no case is set; otherwise only the case that is set is non-`NULL`. A column with no field of its name in the writer's schema reads as `NULL` (and the format logs a warning), so a table declared against a newer schema can read records written with an older one. The format does not check a column's `NOT NULL` on reading: declare `NOT NULL` only for fields that are always present.

**Writing.** A `NULL` column leaves its field unset. For a field without presence the reader then sees the default value, not `NULL`; to round-trip `NULL`, use a field with presence (proto3 `optional`, a wrapper type or a message). In a `oneof` column the first non-`NULL` case is written and the others are ignored; a `oneof` column whose cases are all `NULL` sets none. Protobuf has no `NULL` inside a `repeated` field or a map, so a `NULL` array element or map value fails the row with a `NullPointerException`.

## Well-known types

| Protobuf message | Derived Flink type | Other declared types | Notes |
| --- | --- | --- | --- |
| `google.protobuf.Timestamp` | `TIMESTAMP_LTZ(9)` | `TIMESTAMP(p)`, `BYTES`, `STRING` (JSON) | Nanosecond precision is kept. Instants before 1970 are written with negative `seconds` and `nanos` from 0 to 999,999,999, as `google.protobuf.Timestamp` requires. |
| `google.protobuf.Duration` | `ROW<seconds BIGINT, nanos INT>` | `INTERVAL DAY TO SECOND`, `BYTES`, `STRING` (JSON) | The interval holds milliseconds. |
| `google.type.Date` | `DATE` | `BYTES`, `STRING` (JSON) | Read as a calendar date: a month or day of `0` (a partial date) fails the record. |
| `google.type.TimeOfDay` | `TIME(3)` | `BYTES`, `STRING` (JSON) | Read to the millisecond: nanoseconds below a millisecond are dropped. |
| `confluent.type.Decimal` | `DECIMAL(p, s)` | | See [Confluent annotations](#confluent-annotations). Declare it `DECIMAL`: it has a field named `value`, so `BYTES` and `STRING` columns treat it as a wrapper-style message (see [JSON strings](#json-strings)) rather than holding the whole message. |
| `google.protobuf.DoubleValue`, `FloatValue`, `Int64Value`, `UInt64Value`, `Int32Value`, `UInt32Value`, `BoolValue`, `StringValue`, `BytesValue` | the wrapped type (`DOUBLE`, `FLOAT`, `BIGINT`, `BIGINT`, `INT`, `BIGINT`, `BOOLEAN`, `STRING`, `BYTES`), nullable | `DECIMAL(20, 0)` for `UInt64Value` | An unset wrapper reads as `NULL`, a wrapper holding `0` or `''` as that value. |
| `google.protobuf.Struct`, `Value`, `ListValue` | `STRING` (JSON) | `BYTES` | See [JSON strings](#json-strings). |
| `google.protobuf.Any` | `ROW<type_url STRING, value BYTES>` | `BYTES` | Not unpacked; see [JSON strings](#json-strings). |
| any other message (e.g. `google.protobuf.FieldMask`) | `ROW<...>` of its fields | `BYTES`, `STRING` (JSON) | Like any message of your own. |

## Confluent annotations

Confluent's Protobuf converter records types that Protobuf lacks as `params` of the `(confluent.field_meta)` field option from `confluent/meta.proto`. The format reads and writes these.

```protobuf
import "confluent/meta.proto";
import "confluent/type/decimal.proto";

message Reading {
  int32 level = 1 [(confluent.field_meta) = { params: [
    { key: "connect.type", value: "int8" } ] }];
  int32 count = 2 [(confluent.field_meta) = { params: [
    { key: "connect.type", value: "int16" } ] }];
  confluent.type.Decimal price = 3 [(confluent.field_meta) = { params: [
    { key: "precision", value: "10" }, { key: "scale", value: "2" } ] }];
}
```

derives `` `level` TINYINT, `count` SMALLINT, `price` DECIMAL(10, 2) ``.

- **`connect.type` = `int8` / `int16`** on an `int32`, `sint32` or `sfixed32` field derives `TINYINT` / `SMALLINT`. Reading narrows the 32-bit value to the column's type without a range check, so a value outside it wraps; writing widens it back to `int32`. Without the annotation the field is `INT`, and a table may still declare it `TINYINT` or `SMALLINT`. A sink-derived schema adds the annotation for `TINYINT` and `SMALLINT` columns. Other `connect.type` values are ignored.
- **`precision` / `scale`** on a `confluent.type.Decimal` field derive `DECIMAL(precision, scale)`; a missing (or non-numeric) value falls back to precision `10` and scale `0`, Flink's defaults. Reading takes the message's unscaled `value` bytes with the column's declared scale, without rescaling from the message's own `scale` field, so declare the scale the producer writes with. Writing sets `value`, `precision` and `scale` from the column's value. A sink-derived schema adds both annotations from the column's `DECIMAL(p, s)`.

## Unsigned integers in `BIGINT`

Java has no unsigned 64-bit type, so a `uint64` or `fixed64` value above 9223372036854775807 (`Long.MAX_VALUE`) reads into the derived `BIGINT` column as a negative number: the same 64 bits, read as two's complement (18446744073709551615 reads as `-1`). Writing that negative `BIGINT` back writes the original unsigned value, so values round-trip unchanged, but arithmetic and comparisons in SQL see the negative number. Declare the column `DECIMAL(20, 0)` (see above) to read the unsigned value. `google.protobuf.UInt64Value` behaves the same way.

`uint32` and `fixed32` fit in `BIGINT`: they read as their unsigned value, from 0 to 4294967295, and so does `google.protobuf.UInt32Value`.

## JSON strings

`google.protobuf.Struct`, `Value` and `ListValue` hold arbitrary JSON, and `Struct` and `Value` contain each other, so they have no `ROW` type: the derived DDL maps them to `STRING` and the column holds the value's JSON, e.g. `{"a":1.5,"tags":["x","y"]}`. Before this mapping, deriving DDL for a schema that used them failed with `Cyclic schemas are not supported.` Use Flink's JSON functions (`JSON_VALUE`, `JSON_QUERY`) to read into it.

Any other message field can be declared `STRING` too, and is read and written the same way. The JSON is the canonical proto3 JSON mapping (`com.google.protobuf.util.JsonFormat`) with these settings:

- object keys are the Protobuf field names (`inner_name`, not `innerName`); writing accepts either;
- fields left at their default value are left out, and there is no whitespace;
- writing fails on text that is not valid JSON for the message type, naming the type.

Two kinds of message are not read as JSON:

- **Wrapper-style messages**, those with a field named `value` (`google.protobuf.StringValue` and the other wrappers, and any message of your own shaped like one): a `STRING` column reads and writes that `value` field, as it always has.
- **`google.protobuf.Any`**: its JSON needs the packed message's type, which the format does not know. Read it as `ROW<type_url STRING, value BYTES>` (the derived type).

## Recursive messages

A message that contains itself, directly (`message Tree { Tree child = 1; }`) or through other messages, has no finite `ROW` type, so by default derived DDL (the `ddl` command, the catalog and `ProtoToTableDDL`) fails on it with `Cyclic schemas are not supported.`, naming the message and the field that closes the cycle. Write the DDL for that field by hand, choosing one of:

- **`ROW` nested to a fixed depth.** Declare as many levels as you need; the source reads those levels and leaves out anything deeper, and the sink writes the levels the row has.
- **`BYTES`.** The field's Protobuf encoding, every level included. Decode it later, or write it back to a topic unchanged.
- **`STRING`.** The field's JSON (see [JSON strings](#json-strings)), every level included, for Flink's JSON functions.

```sql
CREATE TABLE trees (
  `name` STRING,
  `child` ROW<`name` STRING, `child` ROW<`name` STRING>>,  -- two levels below the root
  `children` ARRAY<BYTES>                                  -- every level, serialized
) WITH (...);
```

Derived DDL does not do this for you by default, so without the option below a recursive message cannot be read through the catalog; write its table by hand, or set the option.

### Deriving DDL for a recursive message

The `recursive-message` setting makes derived DDL map the field that closes the cycle to one of the three types above instead of failing. Each derived type is one the source reads and the sink writes back, so a table derived this way reads and writes the topic.

| Value | The field that closes the cycle becomes | Levels the table reads and writes |
| --- | --- | --- |
| `fail` (default) | nothing: derivation fails with `Cyclic schemas are not supported.`, naming the message, the field and this option | |
| `bytes` | `BYTES`, the message's Protobuf encoding | every level |
| `json` | `STRING`, the message's JSON | every level |
| `row:<n>` | a `ROW` of the message's fields, unrolled `n` times (`n` ≥ 1); the field is left out of the last level | `n` levels below the table's message |

Set it as:

- the [catalog](../how-to/catalog.md) option `'recursive-message' = 'bytes'`;
- the [`ddl` command](../how-to/generate-ddl.md) flag `--recursive-message bytes`;
- the `RecursionStrategy` argument of `ProtoToTableDDL.tableSchemaFromProto(...)` / `tableSchemaFromDescriptor(...)`, `ProtoToLogicalType.toLogicalType(...)` and `ProtoToSqlType.toSqlType(...)` in Java, e.g. `RecursionStrategy.parse("row:2")`.

For `message Tree { string name = 1; Tree child = 2; repeated Tree children = 3; }`:

| Value | Derived columns |
| --- | --- |
| `bytes` | `` `name` STRING, `child` BYTES, `children` ARRAY<BYTES> NOT NULL `` |
| `json` | `` `name` STRING, `child` STRING, `children` ARRAY<STRING> NOT NULL `` |
| `row:1` | `` `name` STRING, `child` ROW<`name` STRING>, `children` ARRAY<ROW<`name` STRING> NOT NULL> NOT NULL `` |
| `row:2` | `` `name` STRING `` and, for `child` and each element of `children`, `` ROW<`name` STRING, `child` ROW<`name` STRING>, `children` ARRAY<ROW<`name` STRING> NOT NULL> NOT NULL> `` |

Details:

- The cycle closes at the first field that leads back to a message already on the path from the table's message, which counts as the first level. With mutual recursion (`A` contains `B`, which contains `A`), that is the field of `B` that leads back to `A`.
- The setting applies wherever the cycle closes: a plain field, the elements of a `repeated` field and the values of a `map`. With `row:<n>`, a `repeated` or `map` field is left out of the last level like any other, and a message left with no fields is left out too.
- A field inside a `oneof` cannot be left out, because a `oneof` column's fields are matched to its cases by position. With `row:<n>`, such a field becomes `BYTES` at the last level instead, so it keeps every level below it.
- `bytes` and `json` do not apply to a message with a field named `value`, which a `BYTES` or `STRING` column reads like a wrapper type (see [JSON strings](#json-strings)): derivation fails for it and suggests `row:<n>`.
- A `ROW` grows with each level and with each recursive field of the message, so keep `n` small.
- The `message-class` check is unchanged: it accepts all three types for such a field.

## Known limitations

- **No `NULL` for fields without presence.** An unset proto3 scalar (without `optional`), `repeated` field or map reads as its default value, and a `NULL` written to one reads back as that default (see [Nullability](#nullability)). An empty array and a `NULL` array are the same on the wire. A `NULL` array element or map value cannot be written.
- **Unsigned 64-bit integers in `BIGINT`** read values above `Long.MAX_VALUE` as negative numbers; declare `DECIMAL(20, 0)` instead (see [Unsigned integers in `BIGINT`](#unsigned-integers-in-bigint)).
- **`TINYINT` / `SMALLINT`** narrow an `int32` without a range check (see [Confluent annotations](#confluent-annotations)).
- **`DECIMAL`** reads with the column's declared scale, not the scale in each message.
- **Millisecond resolution** for `google.type.TimeOfDay` (`TIME(3)`) and `google.protobuf.Duration` (`INTERVAL DAY TO SECOND`): finer digits are dropped on reading.
- **Partial dates.** A `google.type.Date` with a month or day of `0` fails the record.
- **`google.protobuf.Any`** stays packed: no JSON, no unpacking into the packed message's columns.
- **Recursive messages** need a `recursive-message` setting or a hand-written column (see [Recursive messages](#recursive-messages)).
- **Sink-derived schemas** cannot hold `TIMESTAMP WITH TIME ZONE`, `INTERVAL YEAR TO MONTH`, `RAW` or user-defined types, and never contain enums, `oneof`s or imports other than those of the [types above](#flink-to-protobuf-the-schema-a-sink-derives); register the schema yourself, or pin a `message-class`, when you need those.

Type coverage was extended in [#174](https://github.com/brbrown25/flink-proto-confluent/issues/174). If a Protobuf type you use is missing here or does not convert as described, [open an issue](https://github.com/brbrown25/flink-proto-confluent/issues).
