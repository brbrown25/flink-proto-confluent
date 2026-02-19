package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.utils.DateTimeUtils;

/**
 * Runtime converters between {@link com.google.protobuf.Message} and {@link
 * org.apache.flink.table.data.RowData}. Derived from
 * https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project root.
 */
public class ProtoToRowDataConverters {
  private static final String KEY_FIELD = "key";
  private static final String VALUE_FIELD = "value";

  /**
   * Runtime converter that converts Protobuf data structures into objects of Flink Table &amp; SQL
   * internal data structures.
   */
  @FunctionalInterface
  public interface ProtoToRowDataConverter extends Serializable {
    /**
     * Converts a Protobuf value to Flink internal representation.
     *
     * @param object the Protobuf message or field value
     * @return the Flink internal representation (e.g. RowData, StringData)
     * @throws IOException if conversion fails
     */
    Object convert(Object object) throws IOException;
  }

  /**
   * Creates a converter from the given Protobuf descriptor to the target Flink row type.
   *
   * @param readSchema the Protobuf descriptor for the source message
   * @param targetType the Flink row type for the target RowData
   * @return a converter instance
   */
  public static ProtoToRowDataConverter createConverter(Descriptor readSchema, RowType targetType) {
    if (readSchema.getRealOneofs().isEmpty()) {
      return createNoOneOfRowConverter(readSchema, targetType);
    } else {
      return createOneOfRowConverter(readSchema, targetType);
    }
  }

  private static ProtoToRowDataConverter createOneOfRowConverter(
      Descriptor readSchema, RowType targetType) {
    final Map<String, OneofDescriptor> oneOfDescriptors =
        readSchema.getRealOneofs().stream()
            .collect(Collectors.toMap(OneofDescriptor::getName, Function.identity()));
    final Map<String, FieldDescriptor> fieldDescriptors =
        readSchema.getFields().stream()
            .filter(fieldDescriptor -> fieldDescriptor.getRealContainingOneof() == null)
            .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));

    final List<IndexedFieldConverter> indexedConverters =
        buildIndexedConvertersForOneOf(readSchema, targetType, oneOfDescriptors, fieldDescriptors);
    final int arity = targetType.getFieldCount();

    return message -> {
      GenericRowData row = new GenericRowData(arity);
      Message msg = (Message) message;
      for (IndexedFieldConverter ic : indexedConverters) {
        setRowFieldFromMessage(row, msg, ic);
      }
      return row;
    };
  }

  private static List<IndexedFieldConverter> buildIndexedConvertersForOneOf(
      Descriptor readSchema,
      RowType targetType,
      Map<String, OneofDescriptor> oneOfDescriptors,
      Map<String, FieldDescriptor> fieldDescriptors) {
    final List<IndexedFieldConverter> result = new ArrayList<>(targetType.getFieldCount());
    for (int i = 0; i < targetType.getFieldCount(); i++) {
      RowType.RowField rowField = targetType.getFields().get(i);
      String name = rowField.getName();
      if (oneOfDescriptors.containsKey(name)) {
        OneofDescriptor oneofDescriptor = oneOfDescriptors.get(name);
        ProtoToRowDataConverter converter =
            createConverter(oneofDescriptor, (RowType) rowField.getType());
        result.add(new IndexedFieldConverter(i, oneofDescriptor, null, converter));
      } else {
        FieldDescriptor fieldDescriptor = findFieldDescriptor(fieldDescriptors, name);
        if (fieldDescriptor == null) {
          String protoFields =
              readSchema.getFields().stream()
                  .map(FieldDescriptor::getName)
                  .collect(Collectors.joining(", "));
          throw new IllegalStateException(
              "Table field '"
                  + name
                  + "' not found in proto descriptor. Proto fields: ["
                  + protoFields
                  + "]");
        }
        ProtoToRowDataConverter converter =
            createFieldConverter(fieldDescriptor, rowField.getType());
        result.add(new IndexedFieldConverter(i, null, fieldDescriptor, converter));
      }
    }
    return result;
  }

  private static void setRowFieldFromMessage(
      GenericRowData row, Message message, IndexedFieldConverter ic) throws IOException {
    if (ic.oneofDescriptor != null) {
      if (message.hasOneof(ic.oneofDescriptor)) {
        row.setField(ic.index, ic.converter.convert(message));
      }
      return;
    }
    FieldDescriptor fd = ic.fieldDescriptor;
    if (!fd.hasPresence() || message.hasField(fd)) {
      row.setField(ic.index, ic.converter.convert(message.getField(fd)));
    }
  }

  /**
   * Converter for one field at a fixed row index (preserves table column order for oneof + regular
   * fields).
   */
  private static class IndexedFieldConverter {
    final int index;
    final OneofDescriptor oneofDescriptor;
    final FieldDescriptor fieldDescriptor;
    final ProtoToRowDataConverter converter;

    IndexedFieldConverter(
        int index,
        OneofDescriptor oneofDescriptor,
        FieldDescriptor fieldDescriptor,
        ProtoToRowDataConverter converter) {
      this.index = index;
      this.oneofDescriptor = oneofDescriptor;
      this.fieldDescriptor = fieldDescriptor;
      this.converter = converter;
    }
  }

  /**
   * Resolve proto field by table column name. Tries exact match, then snake_case to camelCase (e.g.
   * organization_domain -> organizationDomain) since Confluent/registry schema may use either.
   */
  private static FieldDescriptor findFieldDescriptor(
      Map<String, FieldDescriptor> byName, String tableFieldName) {
    FieldDescriptor fd = byName.get(tableFieldName);
    if (fd != null) {
      return fd;
    }
    String camelCase = snakeToCamel(tableFieldName);
    return byName.get(camelCase);
  }

  private static String snakeToCamel(String snake) {
    StringBuilder sb = new StringBuilder(snake.length());
    boolean nextUpper = false;
    for (int i = 0; i < snake.length(); i++) {
      char c = snake.charAt(i);
      if (c == '_') {
        nextUpper = true;
      } else {
        sb.append(nextUpper ? Character.toUpperCase(c) : c);
        nextUpper = false;
      }
    }
    return sb.toString();
  }

  private static ProtoToRowDataConverter createNoOneOfRowConverter(
      Descriptor readSchema, RowType targetType) {
    final Map<String, FieldDescriptor> fieldDescriptors =
        readSchema.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));

    final int arity = targetType.getFieldCount();
    final FieldDescriptorWithConverter[] fieldConverters =
        targetType.getFields().stream()
            .map(
                rowField -> {
                  final FieldDescriptor fieldDescriptor =
                      findFieldDescriptor(fieldDescriptors, rowField.getName());
                  if (fieldDescriptor == null) {
                    String protoFields =
                        readSchema.getFields().stream()
                            .map(FieldDescriptor::getName)
                            .collect(Collectors.joining(", "));
                    throw new IllegalStateException(
                        "Table field '"
                            + rowField.getName()
                            + "' not found in proto descriptor. Proto fields: ["
                            + protoFields
                            + "]");
                  }
                  return new FieldDescriptorWithConverter(
                      fieldDescriptor, createFieldConverter(fieldDescriptor, rowField.getType()));
                })
            .toArray(FieldDescriptorWithConverter[]::new);
    return object -> {
      final GenericRowData row = new GenericRowData(arity);
      final Message message = (Message) object;
      for (int i = 0; i < arity; i++) {
        final FieldDescriptor fieldDescriptor = fieldConverters[i].descriptor;
        final ProtoToRowDataConverter converter = fieldConverters[i].converter;
        if (!fieldDescriptor.hasPresence() || message.hasField(fieldDescriptor)) {
          row.setField(i, converter.convert(message.getField(fieldDescriptor)));
        }
      }
      return row;
    };
  }

  private static class FieldDescriptorWithConverter {
    final FieldDescriptor descriptor;
    final ProtoToRowDataConverter converter;

    private FieldDescriptorWithConverter(
        FieldDescriptor descriptor, ProtoToRowDataConverter converter) {
      this.descriptor = descriptor;
      this.converter = converter;
    }
  }

  private static ProtoToRowDataConverter createConverter(
      OneofDescriptor readSchema, RowType targetType) {
    final int arity = targetType.getFieldCount();
    final Map<FieldDescriptor, Pair<ProtoToRowDataConverter, Integer>> fieldConverters =
        new HashMap<>();
    for (int i = 0; i < targetType.getFieldCount(); i++) {
      final FieldDescriptor fieldDescriptor = readSchema.getField(i);
      fieldConverters.put(
          fieldDescriptor,
          Pair.of(createFieldConverter(fieldDescriptor, targetType.getTypeAt(i)), i));
    }
    return object -> {
      final Message message = (Message) object;
      final GenericRowData row = new GenericRowData(arity);
      final FieldDescriptor oneofFieldDescriptor = message.getOneofFieldDescriptor(readSchema);
      final Pair<ProtoToRowDataConverter, Integer> converters =
          fieldConverters.get(oneofFieldDescriptor);
      row.setField(
          converters.getRight(),
          converters.getLeft().convert(message.getField(oneofFieldDescriptor)));
      return row;
    };
  }

  private static ProtoToRowDataConverter createFieldConverter(
      FieldDescriptor readSchema, LogicalType targetType) {
    final Type schemaType = readSchema.getType();
    switch (targetType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return createStringConverter(targetType, schemaType);
      case BOOLEAN:
        return createBooleanConverter(targetType, schemaType);
      case BINARY:
      case VARBINARY:
        return createBinaryConverter(targetType, schemaType);
      case TIME_WITHOUT_TIME_ZONE:
        return createTimeConverter();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return createTimestampConverter();
      case DATE:
        return createDateConverter();
      case DECIMAL:
        return createDecimalConverter((DecimalType) targetType);
      case TINYINT:
        return createTinyIntConverter(targetType, schemaType);
      case SMALLINT:
        return createSmallIntConverter(targetType, schemaType);
      case INTEGER:
        return createIntegerConverter(targetType, schemaType);
      case BIGINT:
        return createBigintConverter(targetType, schemaType);
      case FLOAT:
        return createFloatConverter(targetType, schemaType);
      case DOUBLE:
        return createDoubleConverter(targetType, schemaType);
      case ARRAY:
        return createArrayConverter(readSchema, (ArrayType) targetType);
      case MULTISET:
        return createMultisetConverter(readSchema, (MultisetType) targetType);
      case MAP:
        return createMapConverter(readSchema, (MapType) targetType);
      case ROW:
        return createConverter(readSchema.getMessageType(), (RowType) targetType);
      case NULL:
      case RAW:
      case SYMBOL:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case DISTINCT_TYPE:
      case STRUCTURED_TYPE:
      case INTERVAL_DAY_TIME:
      case INTERVAL_YEAR_MONTH:
      case TIMESTAMP_WITH_TIME_ZONE:
      case UNRESOLVED:
      default:
        throw new IllegalStateException(
            "Couldn't translate unsupported type " + targetType.getTypeRoot() + ".");
    }
  }

  private static ProtoToRowDataConverter createArrayConverter(
      FieldDescriptor readSchema, ArrayType targetType) {
    final ProtoToRowDataConverter elementConverter =
        createFieldConverter(readSchema, targetType.getElementType());
    final Class<?> elementClass =
        LogicalTypeUtils.toInternalConversionClass(targetType.getElementType());
    return object -> {
      final Collection<?> list = (Collection<?>) object;
      final int length = list.size();
      final Object[] array = (Object[]) Array.newInstance(elementClass, length);
      int i = 0;
      for (Object o : list) {
        array[i] = elementConverter.convert(o);
        i++;
      }
      return new GenericArrayData(array);
    };
  }

  private static ProtoToRowDataConverter createMapConverter(
      FieldDescriptor readSchema, MapType targetType) {
    final FieldDescriptor keySchema = readSchema.getMessageType().findFieldByName(KEY_FIELD);
    final FieldDescriptor valueSchema = readSchema.getMessageType().findFieldByName(VALUE_FIELD);
    final ProtoToRowDataConverter keyConverter =
        createFieldConverter(keySchema, targetType.getKeyType());
    final ProtoToRowDataConverter valueConverter =
        createFieldConverter(valueSchema, targetType.getValueType());
    return createMapLikeConverter(keyConverter, valueConverter);
  }

  private static ProtoToRowDataConverter createMultisetConverter(
      FieldDescriptor readSchema, MultisetType targetType) {
    final FieldDescriptor keySchema = readSchema.getMessageType().findFieldByName(KEY_FIELD);
    final FieldDescriptor valueSchema = readSchema.getMessageType().findFieldByName(VALUE_FIELD);
    final ProtoToRowDataConverter keyConverter =
        createFieldConverter(keySchema, targetType.getElementType());
    final ProtoToRowDataConverter valueConverter =
        createFieldConverter(valueSchema, new IntType(false));
    return createMapLikeConverter(keyConverter, valueConverter);
  }

  @SuppressWarnings("unchecked")
  private static ProtoToRowDataConverter createMapLikeConverter(
      ProtoToRowDataConverter keyConverter, ProtoToRowDataConverter valueConverter) {
    return object -> {
      final Collection<? extends Message> protoMap = (Collection<? extends Message>) object;
      final Map<Object, Object> map = new HashMap<>();
      for (Message message : protoMap) {
        final Descriptor descriptor = message.getDescriptorForType();
        final Object elemKey = message.getField(descriptor.findFieldByName(KEY_FIELD));
        final Object elemValue = message.getField(descriptor.findFieldByName(VALUE_FIELD));

        final Object key = keyConverter.convert(elemKey);
        final Object value = valueConverter.convert(elemValue);
        map.put(key, value);
      }
      return new GenericMapData(map);
    };
  }

  private static ProtoToRowDataConverter createStringConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case STRING, ENUM -> object -> StringData.fromString(object.toString());
      case MESSAGE -> object -> StringData.fromString(extractValueField(object).toString());
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createBooleanConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case BOOL -> object -> object;
      case MESSAGE -> object -> extractValueField(object);
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createBinaryConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case BYTES -> object -> ((ByteString) object).toByteArray();
      case MESSAGE -> object -> ((ByteString) extractValueField(object)).toByteArray();
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createTimeConverter() {
    return object -> {
      final Message message = (Message) object;
      int hours = 0;
      int minutes = 0;
      int seconds = 0;
      int nanos = 0;
      for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
        final String name = entry.getKey().getName();
        switch (name) {
          case "hours" -> hours = ((Number) entry.getValue()).intValue();
          case "minutes" -> minutes = ((Number) entry.getValue()).intValue();
          case "seconds" -> seconds = ((Number) entry.getValue()).intValue();
          case "nanos" -> nanos = ((Number) entry.getValue()).intValue();
          default -> throw new IllegalStateException("Unexpected field name: " + name);
        }
      }
      return hours * 3600000 + minutes * 60000 + seconds * 1000 + nanos / 1000_000;
    };
  }

  private static ProtoToRowDataConverter createTinyIntConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case INT32, SINT32, SFIXED32 -> object -> ((Number) object).byteValue();
      case MESSAGE -> object -> ((Number) extractValueField(object)).byteValue();
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createSmallIntConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case INT32, SINT32, SFIXED32 -> object -> ((Number) object).shortValue();
      case MESSAGE -> object -> ((Number) extractValueField(object)).shortValue();
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createIntegerConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case INT32, SINT32, SFIXED32 -> object -> ((Number) object).intValue();
      case MESSAGE -> object -> ((Number) extractValueField(object)).intValue();
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createDoubleConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case DOUBLE -> object -> ((Number) object).doubleValue();
      case MESSAGE -> object -> ((Number) extractValueField(object)).doubleValue();
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createFloatConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case FLOAT -> object -> ((Number) object).floatValue();
      case MESSAGE -> object -> ((Number) extractValueField(object)).floatValue();
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createBigintConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case UINT32, FIXED32, INT64, UINT64, SINT64, FIXED64, SFIXED64 ->
          object -> ((Number) object).longValue();
      case MESSAGE -> object -> ((Number) extractValueField(object)).longValue();
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static IllegalStateException unexpectedTypeForSchema(
      Type schemaType, LogicalTypeRoot flinkType) {
    return new IllegalStateException(
        String.format("Unsupported protobuf type: %s for a SQL type: %s", schemaType, flinkType));
  }

  private static ProtoToRowDataConverter createDecimalConverter(DecimalType targetType) {
    return object -> {
      final ByteString valueField = (ByteString) extractValueField(object);
      return DecimalData.fromUnscaledBytes(
          valueField.toByteArray(), targetType.getPrecision(), targetType.getScale());
    };
  }

  private static ProtoToRowDataConverter createDateConverter() {
    return object -> {
      Message message = (Message) object;
      int year = 0;
      int month = 0;
      int day = 0;
      for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
        final String fieldName = entry.getKey().getName();
        switch (fieldName) {
          case "year" -> year = ((Number) entry.getValue()).intValue();
          case "month" -> month = ((Number) entry.getValue()).intValue();
          case "day" -> day = ((Number) entry.getValue()).intValue();
          default -> throw new IllegalStateException("Unexpected field name: " + fieldName);
        }
      }
      return DateTimeUtils.toInternal(LocalDate.of(year, month, day));
    };
  }

  private static ProtoToRowDataConverter createTimestampConverter() {
    return object -> {
      final Message message = (Message) object;
      long seconds = 0L;
      int nanos = 0;
      for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
        final String fieldName = entry.getKey().getName();
        switch (fieldName) {
          case "seconds" -> seconds = ((Number) entry.getValue()).longValue();
          case "nanos" -> nanos = ((Number) entry.getValue()).intValue();
          default -> throw new IllegalStateException("Unexpected field name: " + fieldName);
        }
      }
      long millis = Math.addExact(Math.multiplyExact(seconds, 1000L), nanos / 1000_000L);
      int nanosOfMillis = nanos % 1000_000;
      return TimestampData.fromEpochMillis(millis, nanosOfMillis);
    };
  }

  private static Object extractValueField(Object value) {
    final Message message = (Message) value;
    final FieldDescriptor fieldDescriptor =
        message.getDescriptorForType().findFieldByName(VALUE_FIELD);

    return message.getField(fieldDescriptor);
  }
}
