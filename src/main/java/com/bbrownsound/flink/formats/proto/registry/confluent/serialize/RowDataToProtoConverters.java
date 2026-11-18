package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.DeclaredTypes;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoJson;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.DynamicMessage.Builder;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.protobuf.type.utils.DecimalUtils;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converters between {@link org.apache.flink.table.data.RowData} and {@link
 * com.google.protobuf.Message}. Derived from https://github.com/amstee/flink-proto-confluent
 * (Apache-2.0). See NOTICE in project root.
 */
public class RowDataToProtoConverters {

  private static final String UINT64_WRAPPER = "google.protobuf.UInt64Value";
  private static final String UINT32_WRAPPER = "google.protobuf.UInt32Value";

  private RowDataToProtoConverters() {}

  /**
   * Runtime converter that converts objects of Flink Table &amp; SQL internal data structures to
   * corresponding Protobuf objects.
   */
  @FunctionalInterface
  public interface RowDataToProtoConverter extends Serializable {
    /**
     * Converts a Flink internal value to Protobuf.
     *
     * @param value the Flink internal value (e.g. from RowData)
     * @return the corresponding Protobuf message or field value
     */
    Object convert(Object value);
  }

  /**
   * Creates a runtime converter according to the given logical type that converts objects of Flink
   * Table &amp; SQL internal data structures to corresponding Protobuf data structures.
   *
   * @param type the Flink row type
   * @param targetSchema the target Protobuf descriptor
   * @return a converter instance
   */
  public static RowDataToProtoConverter createConverter(RowType type, Descriptor targetSchema) {
    validateColumnsPresent(type, targetSchema);
    if (targetSchema.getRealOneofs().isEmpty()) {
      return createNoOneOfConverter(type, targetSchema);
    } else {
      return createOneOfFieldSetter(type, targetSchema);
    }
  }

  /**
   * Fails fast when the table has columns the target descriptor cannot hold, instead of a bare
   * {@link NullPointerException} later. Columns named after a real oneof are matched against the
   * oneof, and the oneof branches are checked against its fields.
   */
  private static void validateColumnsPresent(RowType type, Descriptor targetSchema) {
    final Map<String, OneofDescriptor> oneofs =
        targetSchema.getRealOneofs().stream()
            .collect(Collectors.toMap(OneofDescriptor::getName, Function.identity()));
    final List<String> missing = new ArrayList<>();
    for (RowType.RowField field : type.getFields()) {
      final String name = field.getName();
      if (oneofs.containsKey(name) && field.getType() instanceof RowType) {
        final OneofDescriptor oneof = oneofs.get(name);
        for (RowType.RowField branch : ((RowType) field.getType()).getFields()) {
          final boolean found =
              oneof.getFields().stream().anyMatch(f -> f.getName().equals(branch.getName()));
          if (!found) {
            missing.add(name + "." + branch.getName());
          }
        }
      } else if (targetSchema.findFieldByName(name) == null) {
        missing.add(name);
      }
    }
    if (!missing.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Column(s) %s not found in target Protobuf schema '%s'. The sink is serializing"
                  + " against the registered schema, which lacks these columns. Register a schema"
                  + " that includes them, or pin 'message-class'.",
              missing.stream().map(c -> "'" + c + "'").collect(Collectors.joining(", ")),
              targetSchema.getFullName()));
    }
  }

  private static RowDataToProtoConverter createNoOneOfConverter(
      RowType type, Descriptor targetSchema) {
    final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
    final RowDataToProtoConverter[] fieldConverters =
        type.getFields().stream()
            .map(
                field ->
                    createFieldConverter(
                        field.getType(), targetSchema.findFieldByName(field.getName())))
            .toArray(RowDataToProtoConverter[]::new);
    final LogicalType[] fieldTypes =
        type.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);
    final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
    for (int i = 0; i < fieldTypes.length; i++) {
      fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
    }
    final int length = type.getFieldCount();
    return new RowDataToProtoConverter() {
      @Override
      public Object convert(Object value) {
        final RowData row = (RowData) value;

        final Builder builder = DynamicMessage.newBuilder(targetSchema);
        for (int i = 0; i < length; ++i) {
          String fieldName = fieldNames[i];
          Object field = fieldGetters[i].getFieldOrNull(row);
          if (field != null) {
            builder.setField(
                targetSchema.findFieldByName(fieldName), fieldConverters[i].convert(field));
          }
        }
        return builder.build();
      }
    };
  }

  private static RowDataToProtoConverter createOneOfFieldSetter(
      RowType type, Descriptor targetSchema) {
    final Map<String, OneofDescriptor> oneofDescriptorMap =
        targetSchema.getRealOneofs().stream()
            .collect(Collectors.toMap(OneofDescriptor::getName, Function.identity()));
    final FieldSetter[] fieldSetters =
        type.getFields().stream()
            .map(
                field -> {
                  final String fieldName = field.getName();
                  if (oneofDescriptorMap.containsKey(fieldName)) {
                    return createOneOfFieldSetter(
                        (RowType) field.getType(), oneofDescriptorMap.get(fieldName));
                  } else {
                    return createRegularFieldSetter(
                        field.getType(), targetSchema.findFieldByName(fieldName));
                  }
                })
            .toArray(FieldSetter[]::new);
    final LogicalType[] fieldTypes =
        type.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);
    final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
    for (int i = 0; i < fieldTypes.length; i++) {
      fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
    }
    final int length = type.getFieldCount();
    return new RowDataToProtoConverter() {
      @Override
      public Object convert(Object value) {
        final RowData row = (RowData) value;

        final Builder builder = DynamicMessage.newBuilder(targetSchema);
        for (int i = 0; i < length; ++i) {
          Object field = fieldGetters[i].getFieldOrNull(row);
          if (field != null) {
            fieldSetters[i].setField(builder, field);
          }
        }
        return builder.build();
      }
    };
  }

  /**
   * Helper interface for setting a field of a {@link DynamicMessage} from either a regular field or
   * a field of a {@code oneOf} type which should be flattened in the end result.
   */
  interface FieldSetter {
    void setField(DynamicMessage.Builder builder, Object value);
  }

  private static FieldSetter createRegularFieldSetter(
      LogicalType type, FieldDescriptor descriptor) {
    final RowDataToProtoConverter converter = createFieldConverter(type, descriptor);
    return new FieldSetter() {
      @Override
      public void setField(Builder builder, Object value) {
        if (value != null) {
          final Object converted = converter.convert(value);
          builder.setField(descriptor, converted);
        }
      }
    };
  }

  private static FieldSetter createOneOfFieldSetter(RowType type, OneofDescriptor targetSchema) {
    final Map<String, FieldDescriptor> fieldDescriptors =
        targetSchema.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));
    final FieldDescriptorWithConverter[] fieldConverters =
        type.getFields().stream()
            .map(
                field -> {
                  final FieldDescriptor fieldDescriptor = fieldDescriptors.get(field.getName());
                  return new FieldDescriptorWithConverter(
                      fieldDescriptor, createFieldConverter(field.getType(), fieldDescriptor));
                })
            .toArray(FieldDescriptorWithConverter[]::new);
    final LogicalType[] fieldTypes =
        type.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);
    final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];
    for (int i = 0; i < fieldTypes.length; i++) {
      fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
    }
    return new FieldSetter() {
      @Override
      public void setField(Builder builder, Object value) {
        final RowData row = (RowData) value;
        int nonNullField = -1;
        for (int i = 0; i < fieldGetters.length; i++) {
          if (!row.isNullAt(i)) {
            nonNullField = i;
            break;
          }
        }

        if (nonNullField != -1) {
          final FieldDescriptor descriptor = fieldConverters[nonNullField].descriptor;
          final RowDataToProtoConverter converter = fieldConverters[nonNullField].converter;
          builder.setField(
              descriptor, converter.convert(fieldGetters[nonNullField].getFieldOrNull(row)));
        }
      }
    };
  }

  private static class FieldDescriptorWithConverter {
    final FieldDescriptor descriptor;
    final RowDataToProtoConverter converter;

    private FieldDescriptorWithConverter(
        FieldDescriptor descriptor, RowDataToProtoConverter converter) {
      this.descriptor = descriptor;
      this.converter = converter;
    }
  }

  private static RowDataToProtoConverter createFieldConverter(
      LogicalType type, FieldDescriptor targetSchema) {
    switch (type.getTypeRoot()) {
      case TINYINT:
      case SMALLINT:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return ((Number) value).intValue();
          }
        };
      case BOOLEAN:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case BINARY:
      case VARBINARY:
        if (targetSchema.getType() == Type.MESSAGE) {
          return createWrapperConverter(type, targetSchema);
        } else if (type.getTypeRoot() == org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT
            && (targetSchema.getType() == Type.UINT32
                || targetSchema.getType() == Type.FIXED32
                || targetSchema.getType() == Type.SFIXED32)) {
          return new RowDataToProtoConverter() {
            @Override
            public Object convert(Object value) {
              return value == null ? null : ((Number) value).intValue();
            }
          };
        } else {
          return new RowDataToProtoConverter() {
            @Override
            public Object convert(Object value) {
              return value;
            }
          };
        }
      case CHAR:
      case VARCHAR:
        if (DeclaredTypes.isSerializedMessage(targetSchema)) {
          final Descriptor messageType = targetSchema.getMessageType();
          return value -> ProtoJson.fromJson(value.toString(), messageType);
        } else if (targetSchema.getType() == Type.MESSAGE) {
          return createWrapperConverter(type, targetSchema);
        } else if (targetSchema.getType() == Type.ENUM) {
          return new RowDataToProtoConverter() {
            @Override
            public Object convert(Object value) {
              return targetSchema.getEnumType().findValueByName(value.toString());
            }
          };
        } else {
          return new RowDataToProtoConverter() {
            @Override
            public Object convert(Object value) {
              return value.toString();
            }
          };
        }
      case TIME_WITHOUT_TIME_ZONE:
        return new RowDataToProtoConverter() {
          private static final long serialVersionUID = 1L;

          @Override
          public Object convert(Object object) {
            Integer millisOfDay = (Integer) object;
            final LocalTime localTime = LocalTime.ofNanoOfDay((long) millisOfDay * 1_000_000);
            return TimeOfDay.newBuilder()
                .setHours(localTime.getHour())
                .setMinutes(localTime.getMinute())
                .setSeconds(localTime.getSecond())
                .setNanos(localTime.getNano())
                .build();
          }
        };
      case DATE:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            final Integer intValue = (Integer) value;
            final LocalDate localDate = LocalDate.ofEpochDay(intValue);
            return Date.newBuilder()
                .setYear(localDate.getYear())
                .setMonth(localDate.getMonthValue())
                .setDay(localDate.getDayOfMonth())
                .build();
          }
        };
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        // TIMESTAMP (no time zone) is written as the instant of its wall-clock time in UTC, the
        // inverse of how it is read.
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            TimestampData data = (TimestampData) value;
            final long millisecond = data.getMillisecond();
            final int nanoOfMillisecond = data.getNanoOfMillisecond();

            long seconds = millisecond / 1000;
            int nanos = (int) (millisecond % 1000) * 1_000_000 + nanoOfMillisecond;
            return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
          }
        };
      case DECIMAL:
        if (targetSchema.getType() == Type.UINT64 || targetSchema.getType() == Type.FIXED64) {
          return value -> toUnsignedLong((DecimalData) value, targetSchema);
        }
        if (isMessage(targetSchema, UINT64_WRAPPER)) {
          return value -> UInt64Value.of(toUnsignedLong((DecimalData) value, targetSchema));
        }
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            DecimalData data = (DecimalData) value;
            return DecimalUtils.fromBigDecimal(data.toBigDecimal());
          }
        };
      case ARRAY:
        return createArrayConverter((ArrayType) type, targetSchema);
      case MULTISET:
        return createMultisetConverter((MultisetType) type, targetSchema);
      case MAP:
        return createMapConverter((MapType) type, targetSchema);
      case ROW:
        return createConverter((RowType) type, targetSchema.getMessageType());
      case INTERVAL_DAY_TIME:
      case INTERVAL_YEAR_MONTH:
      case RAW:
      case NULL:
      default:
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }
  }

  private static RowDataToProtoConverter createArrayConverter(
      ArrayType type, FieldDescriptor targetSchema) {
    LogicalType elementType = type.getElementType();
    final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
    final RowDataToProtoConverter elementConverter =
        createFieldConverter(type.getElementType(), targetSchema);
    return new RowDataToProtoConverter() {
      @Override
      public Object convert(Object value) {
        ArrayData arrayData = (ArrayData) value;
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < arrayData.size(); ++i) {
          list.add(elementConverter.convert(elementGetter.getElementOrNull(arrayData, i)));
        }
        return list;
      }
    };
  }

  private static RowDataToProtoConverter createMapConverter(
      MapType type, FieldDescriptor targetSchema) {
    final LogicalType keyType = type.getKeyType();
    final LogicalType valueType = type.getValueType();
    return createMapLikeConverter(targetSchema, valueType, keyType);
  }

  private static RowDataToProtoConverter createMultisetConverter(
      MultisetType type, FieldDescriptor targetSchema) {
    final LogicalType keyType = type.getElementType();
    return createMapLikeConverter(targetSchema, new IntType(false), keyType);
  }

  private static RowDataToProtoConverter createMapLikeConverter(
      FieldDescriptor targetSchema, LogicalType valueType, LogicalType keyType) {
    final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
    final ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
    final FieldDescriptor keyDescr = targetSchema.getMessageType().findFieldByName("key");
    final FieldDescriptor valueDescr = targetSchema.getMessageType().findFieldByName("value");
    final RowDataToProtoConverter valueConverter = createFieldConverter(valueType, valueDescr);
    final RowDataToProtoConverter keyConverter = createFieldConverter(keyType, keyDescr);
    return new RowDataToProtoConverter() {
      private static final long serialVersionUID = 1L;

      @Override
      public Object convert(Object object) {
        final MapData mapData = (MapData) object;
        final ArrayData keyArray = mapData.keyArray();
        final ArrayData valueArray = mapData.valueArray();
        final List<Message> newMapValue = new ArrayList<>();
        for (int i = 0; i < mapData.size(); ++i) {
          final Object key = keyConverter.convert(keyGetter.getElementOrNull(keyArray, i));
          final Object value = valueConverter.convert(valueGetter.getElementOrNull(valueArray, i));
          newMapValue.add(
              DynamicMessage.newBuilder(targetSchema.getMessageType())
                  .setField(keyDescr, key)
                  .setField(valueDescr, value)
                  .build());
        }
        return newMapValue;
      }
    };
  }

  /**
   * Returns the 64 bits of an unsigned integer for a uint64/fixed64 field (Java holds it in a
   * signed long). Fails on a value outside 0 to 2^64 - 1 or with a fractional part rather than
   * wrapping.
   */
  private static long toUnsignedLong(DecimalData value, FieldDescriptor field) {
    final BigDecimal decimal = value.toBigDecimal();
    final BigInteger integer;
    try {
      integer = decimal.toBigIntegerExact();
    } catch (ArithmeticException e) {
      throw unsignedOutOfRange(decimal, field);
    }
    if (integer.signum() < 0 || integer.bitLength() > Long.SIZE) {
      throw unsignedOutOfRange(decimal, field);
    }
    return integer.longValue();
  }

  private static IllegalArgumentException unsignedOutOfRange(
      BigDecimal value, FieldDescriptor field) {
    return new IllegalArgumentException(
        String.format(
            "Value %s does not fit the unsigned 64-bit field '%s': it must be a whole number"
                + " from 0 to 18446744073709551615.",
            value.toPlainString(), field.getFullName()));
  }

  private static boolean isMessage(FieldDescriptor field, String fullName) {
    return field.getType() == Type.MESSAGE && field.getMessageType().getFullName().equals(fullName);
  }

  private static RowDataToProtoConverter createWrapperConverter(
      LogicalType type, FieldDescriptor targetSchema) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return BoolValue.of((Boolean) value);
          }
        };
      case INTEGER:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return Int32Value.of((Integer) value);
          }
        };
      case BIGINT:
        // A BIGINT is derived for the unsigned wrappers too; write the wrapper the field holds.
        if (isMessage(targetSchema, UINT64_WRAPPER)) {
          return value -> UInt64Value.of((Long) value);
        }
        if (isMessage(targetSchema, UINT32_WRAPPER)) {
          return value -> UInt32Value.of(((Long) value).intValue());
        }
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return Int64Value.of((Long) value);
          }
        };
      case FLOAT:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return FloatValue.of((Float) value);
          }
        };
      case DOUBLE:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return DoubleValue.of((Double) value);
          }
        };
      case CHAR:
      case VARCHAR:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return StringValue.of(value.toString());
          }
        };
      case VARBINARY:
      case BINARY:
        return new RowDataToProtoConverter() {
          @Override
          public Object convert(Object value) {
            return BytesValue.of(ByteString.copyFrom((byte[]) value));
          }
        };
      default:
        throw new IllegalStateException(
            "Type " + type + " does not have a wrapper" + " representation.");
    }
  }
}
