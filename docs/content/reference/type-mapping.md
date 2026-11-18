# Type mapping

How the format maps Protobuf types to Flink SQL types, in both directions. The source reads each column from the Protobuf field of the same name, and the sink writes each column to it. The column's type, as the table declares it, decides how the value is converted: a table can declare the type the format derives (below) or, for some fields, one of the alternatives further down.

## Derived types

These are the types the [`ddl` command](../how-to/generate-ddl.md), the [catalog](../how-to/catalog.md) and `ProtoToTableDDL` derive for a Protobuf schema, and the types the sink derives a Protobuf schema from when it registers one.

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

Message-typed fields and wrapper types are nullable; `repeated` fields and maps are `NOT NULL` (an unset repeated field is an empty array). Scalars are nullable in the derived DDL; `--strict-not-null` on the `ddl` command makes them `NOT NULL`.

## Declared alternatives

A table can declare one of these types for a field instead of the derived one. The source reads the field into it, the sink writes it back to the same Protobuf value, and the `message-class` check accepts it. Derived DDL never produces them: declare the column yourself, or edit the generated DDL.

| Protobuf type | Declared Flink SQL type | Conversion |
| --- | --- | --- |
| `google.protobuf.Timestamp` | `TIMESTAMP(p)` | The instant's wall-clock time in UTC. Writing reads the column's wall-clock time as UTC. When the sink derives a schema, a `TIMESTAMP(p)` column becomes a `google.protobuf.Timestamp` field. |
| `uint64`, `fixed64`, `google.protobuf.UInt64Value` | `DECIMAL(p, s)`, normally `DECIMAL(20, 0)` | The unsigned value, from 0 to 18446744073709551615. `DECIMAL(20, 0)` holds every value; with a smaller precision a value that does not fit reads as `NULL`. Writing fails on a negative value, a fractional value or one above 18446744073709551615 instead of wrapping it. |
| any `message` except `google.protobuf.Any` and wrapper-style messages | `STRING` | The message's JSON (see [JSON strings](#json-strings)). |
| `enum` | `INT` | The value's number instead of its name. A number the enum does not define (written by a newer schema) reads as that number. Writing a number the enum does not define keeps it in an open (proto3) enum and fails for a closed (proto2) enum, which cannot hold it. Works for `repeated` enums (`ARRAY<INT>`) and map values too. |

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
