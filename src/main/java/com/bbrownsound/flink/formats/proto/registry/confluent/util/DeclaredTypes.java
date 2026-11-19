package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Column types a table may declare for a Protobuf field in place of the one {@link ProtoToSqlType}
 * derives. The converters read and write these whenever the table declares them; derived DDL (the
 * catalog, the {@code ddl} command and {@link ProtoToTableDDL}) never produces them, so a table
 * only gets one by asking for it. The one exception is a field that makes a message contain itself,
 * which derived DDL maps to {@code BYTES}, {@code STRING} or a {@code ROW} nested to a fixed depth
 * when asked to by {@link RecursionStrategy}.
 *
 * <ul>
 *   <li>{@code google.protobuf.Timestamp} as {@code TIMESTAMP(p)}: the instant's wall-clock time in
 *       UTC.
 *   <li>{@code uint64}, {@code fixed64} and {@code google.protobuf.UInt64Value} as {@code
 *       DECIMAL(p, s)}: the unsigned value. {@code DECIMAL(20, 0)} holds every value; the sink
 *       fails on a value outside 0 to 2^64 - 1 instead of wrapping it.
 *   <li>Any message (other than {@code google.protobuf.Any} and wrapper-style messages, see {@link
 *       #isSerializedMessage}) as {@code STRING}: the message's proto3 JSON, see {@link ProtoJson}.
 *       Derived DDL does this for {@code google.protobuf.Struct}, {@code Value} and {@code
 *       ListValue}.
 *   <li>An {@code enum} as {@code INT}: the value's number. A number the enum does not define reads
 *       as itself; writing one keeps it in an open (proto3) enum and fails for a closed (proto2)
 *       one.
 *   <li>Any message (other than wrapper-style messages, see {@link #isSerializedMessage}) as {@code
 *       BYTES}: the message's Protobuf encoding. With {@code STRING} and a {@code ROW} nested to a
 *       fixed depth, this is how a table reads a recursive message.
 *   <li>{@code google.protobuf.Duration} as {@code INTERVAL DAY TO SECOND}: the duration in
 *       milliseconds, the interval's resolution; nanoseconds below a millisecond are dropped.
 * </ul>
 */
public final class DeclaredTypes {

  private DeclaredTypes() {}

  /**
   * Returns whether a column of the given type reads and writes one value of the field (an element,
   * for a repeated field) through one of the alternatives listed on this class.
   *
   * @param field the Protobuf field
   * @param type the declared column (or element) type
   * @return true if the converters accept the type for the field as an alternative mapping
   */
  public static boolean isAlternative(FieldDescriptor field, LogicalType type) {
    switch (type.getTypeRoot()) {
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return isMessage(field, CommonConstants.PROTOBUF_TIMESTAMP_TYPE);
      case INTERVAL_DAY_TIME:
        return isMessage(field, CommonConstants.PROTOBUF_DURATION_TYPE);
      case DECIMAL:
        return field.getType() == FieldDescriptor.Type.UINT64
            || field.getType() == FieldDescriptor.Type.FIXED64
            || isMessage(field, CommonConstants.PROTOBUF_UINT64_WRAPPER_TYPE);
      case INTEGER:
        return field.getType() == FieldDescriptor.Type.ENUM;
      case BINARY:
      case VARBINARY:
        return isSerializedMessage(field);
      case CHAR:
      case VARCHAR:
        return isSerializedMessage(field) && !isMessage(field, CommonConstants.PROTOBUF_ANY_TYPE);
      default:
        return false;
    }
  }

  /**
   * Returns whether a message field declared {@code STRING} (or {@code BYTES}) holds the whole
   * message, serialized, rather than the {@code value} field of a wrapper. A message with a field
   * named {@code value} ({@code google.protobuf.StringValue} and the other wrappers, and any
   * message shaped like one) keeps being read through that field, as it always has; {@code
   * google.protobuf.Any} is not a wrapper even though it has one.
   *
   * @param field the Protobuf field
   * @return true if the field is a message the converters serialize whole
   */
  public static boolean isSerializedMessage(FieldDescriptor field) {
    return ProtoToSqlType.isSerializedMessage(field);
  }

  private static boolean isMessage(FieldDescriptor field, String fullName) {
    return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
        && field.getMessageType().getFullName().equals(fullName);
  }
}
