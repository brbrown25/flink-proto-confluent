package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoSqlType.Field;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoSqlType.Kind;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Maps a protobuf {@link Descriptor} to the {@link ProtoSqlType} of its Flink table row, without
 * depending on Flink. This is the one place that decides the mapping; {@link ProtoToLogicalType}
 * converts the result to Flink's {@code LogicalType}. Derived from
 * https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project root.
 *
 * <p>The mapping is represented by the following table:
 *
 * <pre>
 * +------------------+-----------------------------------+--------------------+------------------+
 * | Protobuf type    | Message type                      | Connect annotation | Flink type       |
 * +------------------+-----------------------------------+--------------------+------------------+
 * | INT32|SINT32|..  | -                                 |                    | INT              |
 * | INT32|SINT32|..  | -                                 | int8               | TINYINT          |
 * | INT32|SINT32|..  | -                                 | int16              | SMALLINT         |
 * | UINT32|FIXED32|..| -                                 | -                  | BIGINT           |
 * | FLOAT            | -                                 | -                  | FLOAT            |
 * | DOUBLE           | -                                 | -                  | DOUBLE           |
 * | BOOL             | -                                 | -                  | BOOLEAN          |
 * | ENUM             | -                                 | -                  | VARCHAR          |
 * | STRING           | -                                 | -                  | VARCHAR          |
 * | BYTES            | -                                 | -                  | VARBINARY        |
 * | MESSAGE          | confluent.type.Decimal            | -                  | DECIMAL          |
 * | MESSAGE          | google.type.Date                  | -                  | DATE             |
 * | MESSAGE          | google.type.TimeOfDay             | -                  | TIME(3)          |
 * | MESSAGE          | google.protobuf.Timestamp         | -                  | TIMESTAMP_LTZ(9) |
 * | MESSAGE          | google.protobuf.DoubleValue       | -                  | DOUBLE           |
 * | MESSAGE          | google.protobuf.FloatValue        | -                  | FLOAT            |
 * | MESSAGE          | google.protobuf.Int32Value        | -                  | INT              |
 * | MESSAGE          | google.protobuf.Int64Value        | -                  | BIGINT           |
 * | MESSAGE          | google.protobuf.UInt64Value       | -                  | BIGINT           |
 * | MESSAGE          | google.protobuf.UInt32Value       | -                  | BIGINT           |
 * | MESSAGE          | google.protobuf.BoolValue         | -                  | BOOLEAN          |
 * | MESSAGE          | google.protobuf.StringValue       | -                  | VARCHAR          |
 * | MESSAGE          | google.protobuf.BytesValue        | -                  | VARBINARY        |
 * | MESSAGE          | google.protobuf.Struct            | -                  | VARCHAR (JSON)   |
 * | MESSAGE          | google.protobuf.Value             | -                  | VARCHAR (JSON)   |
 * | MESSAGE          | google.protobuf.ListValue         | -                  | VARCHAR (JSON)   |
 * | MESSAGE          | repeated xx.xx.XXEntry(key, val)  | -                  | MAP[type1, type2] |
 * | MESSAGE          | -                                 | -                  | ROW              |
 * | oneOf            | -                                 | -                  | ROW              |
 * +------------------+-----------------------------------+--------------------+------------------+
 * </pre>
 *
 * <p>Expressing something as NULLABLE or NOT NULL is not straightforward in Protobuf.
 *
 * <ul>
 *   <li>all non MESSAGE types are NOT NULL (if not set explicitly default value is assigned)
 *   <li>non MESSAGE types marked with 'optional' can be checked if they were set. If not set we
 *       assume NULL
 *   <li>MESSAGE types are all NULLABLE, in other words all fields of a MESSAGE type are optional
 *       and there is no way to ensure on a format level they are NOT NULL
 *   <li>ARRAYS can not be NULL, not set repeated field is presented as an empty list, there is no
 *       way to differentiate an empty array from NULL
 * </ul>
 */
public final class ProtoToSqlType {

  /** Precision of a decimal without a precision annotation (Flink's DecimalType default). */
  static final int DEFAULT_DECIMAL_PRECISION = 10;

  /** Scale of a decimal without a scale annotation (Flink's DecimalType default). */
  static final int DEFAULT_DECIMAL_SCALE = 0;

  /** Precision of {@code google.type.TimeOfDay} columns. */
  static final int TIME_PRECISION = 3;

  /** Precision of {@code google.protobuf.Timestamp} columns. */
  static final int TIMESTAMP_PRECISION = 9;

  private ProtoToSqlType() {}

  /**
   * Mostly adapted the logic from <a
   * href="https://github.com/confluentinc/schema-registry/blob/610fbed58a3a8d778ec7a9de5b8d2d0c1465c6f9/protobuf-converter/src/main/java/io/confluent/connect/protobuf/ProtobufData.java">ProtobufData</a>
   * Should be kept in sync to handle all connect data types.
   *
   * @param schema the Protobuf descriptor
   * @return the row type of the message (the top-level row is not nullable)
   * @throws IllegalArgumentException if the schema is cyclic
   */
  public static ProtoSqlType toSqlType(final Descriptor schema) {
    final CycleContext context = new CycleContext();
    // top-level row must not be NULLABLE in SQL, thus we change the nullability of the top row
    return toRowNested(schema, context).withNullable(false);
  }

  /**
   * Returns the SQL type of a single value of the given field, ignoring {@code repeated}: for a
   * {@code repeated int64} field this is {@code BIGINT}, the element type of the array the field
   * maps to.
   *
   * @param field the Protobuf field descriptor
   * @return the SQL type of one value of the field
   * @throws IllegalArgumentException if the field's message type is cyclic
   */
  public static ProtoSqlType toValueSqlType(final FieldDescriptor field) {
    return convertNonRepeated(field, new CycleContext());
  }

  private static ProtoSqlType toRowNested(final Descriptor schema, final CycleContext context) {
    final List<Field> fields = new ArrayList<>();
    final List<Field> oneOfFields = new ArrayList<>();
    for (OneofDescriptor oneOfDescriptor : schema.getRealOneofs()) {
      oneOfFields.add(new Field(oneOfDescriptor.getName(), toSqlType(oneOfDescriptor, context)));
    }
    for (FieldDescriptor fieldDescriptor : schema.getFields()) {
      if (fieldDescriptor.getRealContainingOneof() != null) {
        // Already added field as oneof
        continue;
      }
      fields.add(new Field(fieldDescriptor.getName(), toSqlType(fieldDescriptor, context)));
    }
    fields.addAll(oneOfFields);
    return ProtoSqlType.row(true, fields);
  }

  private static ProtoSqlType toSqlType(OneofDescriptor oneOfDescriptor, CycleContext context) {
    final List<Field> fields = new ArrayList<>();
    for (FieldDescriptor field : oneOfDescriptor.getFields()) {
      fields.add(new Field(field.getName(), toSqlType(field, context)));
    }
    return ProtoSqlType.row(true, fields);
  }

  private static ProtoSqlType toSqlType(final FieldDescriptor schema, final CycleContext context) {
    if (schema.isRepeated()) {
      return convertRepeated(schema, context);
    } else {
      return convertNonRepeated(schema, context);
    }
  }

  private static ProtoSqlType convertNonRepeated(
      FieldDescriptor schema, CycleContext cycleContext) {
    final boolean isOptional = !schema.isRequired();
    switch (schema.getType()) {
      case INT32:
      case SINT32:
      case SFIXED32:
        return convertInt32Family(schema, isOptional);
      case UINT32:
      case FIXED32:
      case INT64:
      case UINT64:
      case SINT64:
      case FIXED64:
      case SFIXED64:
        return ProtoSqlType.of(Kind.BIGINT, isOptional);
      case FLOAT:
        return ProtoSqlType.of(Kind.FLOAT, isOptional);
      case DOUBLE:
        return ProtoSqlType.of(Kind.DOUBLE, isOptional);
      case BOOL:
        return ProtoSqlType.of(Kind.BOOLEAN, isOptional);
      case ENUM:
      case STRING:
        return ProtoSqlType.of(Kind.STRING, isOptional);
      case BYTES:
        return ProtoSqlType.of(Kind.BYTES, isOptional);
      case MESSAGE:
        return convertMessageType(schema, cycleContext);
      default:
        throw new IllegalArgumentException("Unknown schema type: " + schema.getType());
    }
  }

  private static ProtoSqlType convertInt32Family(FieldDescriptor schema, boolean isOptional) {
    if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
      Meta fieldMeta = schema.getOptions().getExtension(MetaProto.fieldMeta);
      String connectType = fieldMeta.getParamsMap().get(CommonConstants.CONNECT_TYPE_PROP);
      if (CommonConstants.CONNECT_TYPE_INT8.equals(connectType)) {
        return ProtoSqlType.of(Kind.TINYINT, isOptional);
      }
      if (CommonConstants.CONNECT_TYPE_INT16.equals(connectType)) {
        return ProtoSqlType.of(Kind.SMALLINT, isOptional);
      }
    }
    return ProtoSqlType.of(Kind.INT, isOptional);
  }

  private static ProtoSqlType convertMessageType(
      FieldDescriptor schema, CycleContext cycleContext) {
    String fullName = schema.getMessageType().getFullName();
    switch (fullName) {
      case CommonConstants.PROTOBUF_DECIMAL_TYPE:
        return decimalTypeFromMeta(schema);
      case CommonConstants.PROTOBUF_DATE_TYPE:
        return ProtoSqlType.of(Kind.DATE, true);
      case CommonConstants.PROTOBUF_TIME_TYPE:
        return ProtoSqlType.time(true, TIME_PRECISION);
      case CommonConstants.PROTOBUF_TIMESTAMP_TYPE:
        return ProtoSqlType.timestampLtz(true, TIMESTAMP_PRECISION);
      case CommonConstants.PROTOBUF_STRUCT_TYPE:
      case CommonConstants.PROTOBUF_VALUE_TYPE:
      case CommonConstants.PROTOBUF_LIST_VALUE_TYPE:
        // Dynamic JSON: Struct and Value contain each other, so they have no finite ROW type.
        return ProtoSqlType.of(Kind.STRING, true);
      default:
        return toRecordSchemaWithCycleCheck(schema, cycleContext, fullName);
    }
  }

  private static ProtoSqlType decimalTypeFromMeta(FieldDescriptor schema) {
    int precision = DEFAULT_DECIMAL_PRECISION;
    int scale = DEFAULT_DECIMAL_SCALE;
    if (schema.getOptions().hasExtension(MetaProto.fieldMeta)) {
      Map<String, String> params =
          schema.getOptions().getExtension(MetaProto.fieldMeta).getParamsMap();
      precision = parseMetaInt(params.get(CommonConstants.PROTOBUF_PRECISION_PROP), precision);
      scale = parseMetaInt(params.get(CommonConstants.PROTOBUF_SCALE_PROP), scale);
    }
    return ProtoSqlType.decimal(true, precision, scale);
  }

  private static int parseMetaInt(String value, int defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static ProtoSqlType toRecordSchemaWithCycleCheck(
      FieldDescriptor schema, CycleContext cycleContext, String fullName) {
    if (cycleContext.seenMessage.contains(fullName)) {
      throw new IllegalArgumentException("Cyclic schemas are not supported.");
    }
    cycleContext.seenMessage.add(fullName);
    try {
      return toUnwrappedOrRecordSchema(schema, cycleContext);
    } finally {
      cycleContext.seenMessage.remove(fullName);
    }
  }

  private static ProtoSqlType convertRepeated(FieldDescriptor schema, CycleContext context) {
    if (isMapDescriptor(schema)) {
      return toMapSchema(schema.getMessageType(), context);
    } else {
      // repeated type is always NOT NULL; array elements (ROW) are NOT NULL by convention
      ProtoSqlType elementType = convertNonRepeated(schema, context);
      if (elementType.getKind() == Kind.ROW) {
        elementType = elementType.withNullable(false);
      }
      return ProtoSqlType.array(false, elementType);
    }
  }

  private static ProtoSqlType toUnwrappedOrRecordSchema(
      FieldDescriptor descriptor, CycleContext context) {
    return toUnwrappedSchema(descriptor.getMessageType())
        .orElseGet(() -> toRowNested(descriptor.getMessageType(), context));
  }

  private static Optional<ProtoSqlType> toUnwrappedSchema(Descriptor descriptor) {
    switch (descriptor.getFullName()) {
      case CommonConstants.PROTOBUF_DOUBLE_WRAPPER_TYPE:
        return Optional.of(ProtoSqlType.of(Kind.DOUBLE, true));
      case CommonConstants.PROTOBUF_FLOAT_WRAPPER_TYPE:
        return Optional.of(ProtoSqlType.of(Kind.FLOAT, true));
      case CommonConstants.PROTOBUF_INT64_WRAPPER_TYPE:
      case CommonConstants.PROTOBUF_UINT64_WRAPPER_TYPE:
      case CommonConstants.PROTOBUF_UINT32_WRAPPER_TYPE:
        return Optional.of(ProtoSqlType.of(Kind.BIGINT, true));
      case CommonConstants.PROTOBUF_INT32_WRAPPER_TYPE:
        return Optional.of(ProtoSqlType.of(Kind.INT, true));
      case CommonConstants.PROTOBUF_BOOL_WRAPPER_TYPE:
        return Optional.of(ProtoSqlType.of(Kind.BOOLEAN, true));
      case CommonConstants.PROTOBUF_STRING_WRAPPER_TYPE:
        return Optional.of(ProtoSqlType.of(Kind.STRING, true));
      case CommonConstants.PROTOBUF_BYTES_WRAPPER_TYPE:
        return Optional.of(ProtoSqlType.of(Kind.BYTES, true));
      default:
        return Optional.empty();
    }
  }

  private static ProtoSqlType toMapSchema(final Descriptor descriptor, CycleContext context) {
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    // repeated type is always NOT NULL
    return ProtoSqlType.map(
        false,
        toSqlType(fieldDescriptors.get(0), context),
        toSqlType(fieldDescriptors.get(1), context));
  }

  private static boolean isMapDescriptor(FieldDescriptor fieldDescriptor) {
    if (fieldDescriptor.getType() != Type.MESSAGE) {
      return false;
    }
    Descriptor descriptor = fieldDescriptor.getMessageType();
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    return descriptor.getName().endsWith(CommonConstants.MAP_ENTRY_SUFFIX)
        && fieldDescriptors.size() == 2
        && fieldDescriptors.get(0).getName().equals(CommonConstants.KEY_FIELD)
        && fieldDescriptors.get(1).getName().equals(CommonConstants.VALUE_FIELD)
        && !fieldDescriptors.get(0).isRepeated()
        && !fieldDescriptors.get(1).isRepeated();
  }

  private static final class CycleContext {
    private final Set<String> seenMessage = new HashSet<>();
  }
}
