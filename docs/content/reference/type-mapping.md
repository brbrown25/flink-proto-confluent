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

## Unsigned integers in `BIGINT`

Java has no unsigned 64-bit type, so a `uint64` or `fixed64` value above 9223372036854775807 (`Long.MAX_VALUE`) reads into the derived `BIGINT` column as a negative number: the same 64 bits, read as two's complement (18446744073709551615 reads as `-1`). Writing that negative `BIGINT` back writes the original unsigned value, so values round-trip unchanged, but arithmetic and comparisons in SQL see the negative number. Declare the column `DECIMAL(20, 0)` (see above) to read the unsigned value. `google.protobuf.UInt64Value` behaves the same way.

`uint32` and `fixed32` fit in `BIGINT`: they read as their unsigned value, from 0 to 4294967295, and so does `google.protobuf.UInt32Value`.
