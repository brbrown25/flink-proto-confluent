package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Column types a table may declare for a Protobuf field in place of the one {@link ProtoToSqlType}
 * derives. The converters read and write these whenever the table declares them; derived DDL (the
 * catalog, the {@code ddl} command and {@link ProtoToTableDDL}) never produces them, so a table
 * only gets one by asking for it.
 *
 * <ul>
 *   <li>{@code google.protobuf.Timestamp} as {@code TIMESTAMP(p)}: the instant's wall-clock time in
 *       UTC.
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
      default:
        return false;
    }
  }

  private static boolean isMessage(FieldDescriptor field, String fullName) {
    return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
        && field.getMessageType().getFullName().equals(fullName);
  }
}
