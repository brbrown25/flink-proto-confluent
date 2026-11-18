package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * A converter from {@link com.google.protobuf.Descriptors.Descriptor} to {@link
 * org.apache.flink.table.types.logical.LogicalType}. Derived from
 * https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project root.
 *
 * <p>The protobuf-to-SQL mapping itself, including the mapping table and the nullability rules,
 * lives in {@link ProtoToSqlType}, which does not depend on Flink; this class converts its result
 * to Flink's type classes. STRING maps to {@code VARCHAR(MAX)} and BYTES to {@code
 * VARBINARY(MAX)}.
 */
public class ProtoToLogicalType {

  private ProtoToLogicalType() {}

  /**
   * Returns the Flink row type of a protobuf message, as described in {@link ProtoToSqlType}.
   *
   * @param schema the Protobuf descriptor
   * @return the corresponding Flink logical type (top-level row is not nullable)
   */
  public static LogicalType toLogicalType(final Descriptor schema) {
    return toLogicalType(ProtoToSqlType.toSqlType(schema));
  }

  /**
   * Returns the Flink type of a single value of the given field, ignoring {@code repeated}: for a
   * {@code repeated int64} field this is {@code BIGINT}, the element type of the array the field
   * maps to.
   *
   * @param field the Protobuf field descriptor
   * @return the corresponding Flink logical type of one value of the field
   * @throws IllegalArgumentException if the field's message type is cyclic
   */
  public static LogicalType toValueLogicalType(final FieldDescriptor field) {
    return toLogicalType(ProtoToSqlType.toValueSqlType(field));
  }

  /**
   * Converts a {@link ProtoSqlType} to the corresponding Flink logical type.
   *
   * @param type the type to convert
   * @return the Flink logical type, with the same nullability
   */
  public static LogicalType toLogicalType(final ProtoSqlType type) {
    final boolean nullable = type.isNullable();
    switch (type.getKind()) {
      case BOOLEAN:
        return new BooleanType(nullable);
      case TINYINT:
        return new TinyIntType(nullable);
      case SMALLINT:
        return new SmallIntType(nullable);
      case INT:
        return new IntType(nullable);
      case BIGINT:
        return new BigIntType(nullable);
      case FLOAT:
        return new FloatType(nullable);
      case DOUBLE:
        return new DoubleType(nullable);
      case DECIMAL:
        return new DecimalType(nullable, type.getPrecision(), type.getScale());
      case STRING:
        return new VarCharType(nullable, VarCharType.MAX_LENGTH);
      case BYTES:
        return new VarBinaryType(nullable, VarBinaryType.MAX_LENGTH);
      case DATE:
        return new DateType(nullable);
      case TIME:
        return new TimeType(nullable, type.getPrecision());
      case TIMESTAMP_LTZ:
        return new LocalZonedTimestampType(nullable, type.getPrecision());
      case ROW:
        final List<RowField> fields = new ArrayList<>(type.getFields().size());
        for (ProtoSqlType.Field field : type.getFields()) {
          fields.add(new RowField(field.getName(), toLogicalType(field.getType())));
        }
        return new RowType(nullable, fields);
      case ARRAY:
        return new ArrayType(nullable, toLogicalType(type.getElementType()));
      case MAP:
        return new MapType(
            nullable, toLogicalType(type.getKeyType()), toLogicalType(type.getValueType()));
      default:
        throw new IllegalArgumentException("Unhandled SQL type: " + type);
    }
  }
}
