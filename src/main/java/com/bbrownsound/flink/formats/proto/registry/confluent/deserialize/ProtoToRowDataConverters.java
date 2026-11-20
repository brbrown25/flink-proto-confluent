package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.DeclaredTypes;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoJson;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime converters between {@link com.google.protobuf.Message} and {@link
 * org.apache.flink.table.data.RowData}. Derived from
 * https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project root.
 */
public class ProtoToRowDataConverters {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoToRowDataConverters.class);
  private static final String KEY_FIELD = "key";
  private static final String VALUE_FIELD = "value";
  private static final String UINT32_WRAPPER = "google.protobuf.UInt32Value";

  private ProtoToRowDataConverters() {}

  /**
   * Records that a table column has no counterpart in the writer's Protobuf descriptor. The column
   * is read as NULL rather than failing the job, so a table declared against a newer schema version
   * can still read records written under an older one (a field added in v2 is simply absent from
   * v1 payloads). A genuine column-name typo shows up as an always-null column plus this warning.
   */
  private static void logMissingField(Descriptor readSchema, String tableFieldName) {
    LOG.warn(
        "[proto-confluent] Table field '{}' not found in proto descriptor '{}' (proto fields: [{}])"
            + " - reading it as NULL",
        tableFieldName,
        readSchema.getFullName(),
        readSchema.getFields().stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.joining(", ")));
  }

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

  /**
   * Creates a converter that produces a nested projection of a table: one column per projection
   * path, holding the value at the end of that path. Each path is resolved against the message's
   * fields once, here; a record is then read by walking those fields, and only the projected values
   * are converted, so the sibling fields of a projected nested field cost nothing.
   *
   * <p>A column is NULL when a message along its path is unset (as reading the enclosing {@code
   * ROW} column and then its field would give), when its oneof case is not the one set, and when
   * the writer's message has no field for it (as for a top-level column, see {@link
   * #createConverter(Descriptor, RowType)}).
   *
   * @param readSchema the Protobuf descriptor for the source message
   * @param physicalType the table's physical row type the projection paths index into
   * @param projections the projected paths, one per produced column, as Flink's {@code Projection}
   *     holds them
   * @return a converter producing a row of the projected columns, in projection order
   * @throws IllegalStateException if a column along a path cannot be read from its field
   */
  public static ProtoToRowDataConverter createProjectedConverter(
      Descriptor readSchema, RowType physicalType, int[][] projections) {
    final int arity = projections.length;
    final ProjectedReader[] readers = new ProjectedReader[arity];
    for (int i = 0; i < arity; i++) {
      readers[i] = createProjectedReader(ProjectedPath.resolve(readSchema, physicalType, projections[i]));
    }
    return object -> {
      final GenericRowData row = new GenericRowData(arity);
      final Message message = (Message) object;
      for (int i = 0; i < arity; i++) {
        row.setField(i, readers[i].read(message));
      }
      return row;
    };
  }

  /** Reads one projected column from the top-level message. */
  @FunctionalInterface
  private interface ProjectedReader {
    Object read(Message message) throws IOException;
  }

  private static ProjectedReader createProjectedReader(ProjectedPath path) {
    if (path.problem != null) {
      if (!path.missing) {
        throw new IllegalStateException(path.problem);
      }
      LOG.warn("[proto-confluent] Projected {} - reading it as NULL", path.problem);
      return message -> null;
    }
    final FieldDescriptor[] fields = path.fields;
    if (path.oneof != null) {
      final OneofDescriptor oneof = path.oneof;
      final ProtoToRowDataConverter converter = createConverter(oneof, (RowType) path.type);
      return message -> {
        final Message holder = descend(message, fields, fields.length);
        return holder != null && holder.hasOneof(oneof) ? converter.convert(holder) : null;
      };
    }
    final FieldDescriptor leaf = fields[fields.length - 1];
    final ProtoToRowDataConverter converter = createFieldConverter(leaf, path.type);
    return message -> {
      final Message holder = descend(message, fields, fields.length - 1);
      if (holder == null || (leaf.hasPresence() && !holder.hasField(leaf))) {
        return null;
      }
      return converter.convert(holder.getField(leaf));
    };
  }

  /**
   * Follows the first {@code count} fields, each a singular message field, from {@code message}.
   *
   * @return the message the fields lead to, or {@code null} if one of them is unset
   */
  private static Message descend(Message message, FieldDescriptor[] fields, int count) {
    Message current = message;
    for (int k = 0; k < count; k++) {
      if (!current.hasField(fields[k])) {
        return null;
      }
      current = (Message) current.getField(fields[k]);
    }
    return current;
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
          logMissingField(readSchema, name);
          result.add(new IndexedFieldConverter(i, null, null, null));
          continue;
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
    if (fd == null) {
      // Column absent from the writer's schema (e.g. a field added in a later schema version
      // reading records written under an older one): leave it null.
      return;
    }
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
  static FieldDescriptor findFieldDescriptor(
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
                    logMissingField(readSchema, rowField.getName());
                    return new FieldDescriptorWithConverter(null, null);
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
        if (fieldDescriptor == null) {
          // Column absent from the writer's schema (e.g. a field added in a later schema version
          // reading records written under an older one): leave it null.
          continue;
        }
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
    // Iterate only up to the oneof's field count; table schema may have more columns (e.g. 28
    // when proto oneof has 27). Extra columns are left null.
    final int oneofFieldCount = readSchema.getFieldCount();
    for (int i = 0; i < targetType.getFieldCount() && i < oneofFieldCount; i++) {
      final FieldDescriptor fieldDescriptor = readSchema.getField(i);
      fieldConverters.put(
          fieldDescriptor,
          Pair.of(createFieldConverter(fieldDescriptor, targetType.getTypeAt(i)), i));
    }
    return object -> {
      final Message message = (Message) object;
      final GenericRowData row = new GenericRowData(arity);
      final FieldDescriptor oneofFieldDescriptor = message.getOneofFieldDescriptor(readSchema);
      // getOneofFieldDescriptor returns null when no case of the oneof is set. The descriptor
      // may also be absent from fieldConverters when the proto oneof has gained a case that is
      // not present in the target table schema (schema drift). In either case, leave the row's
      // columns null instead of dereferencing a null converter (which previously NPE'd and, with
      // no error tolerance on the Kafka source, crash-looped the whole job on a single record).
      final Pair<ProtoToRowDataConverter, Integer> converters =
          oneofFieldDescriptor == null ? null : fieldConverters.get(oneofFieldDescriptor);
      if (converters != null) {
        row.setField(
            converters.getRight(),
            converters.getLeft().convert(message.getField(oneofFieldDescriptor)));
      }
      return row;
    };
  }

  private static ProtoToRowDataConverter createFieldConverter(
      FieldDescriptor readSchema, LogicalType targetType) {
    final Type schemaType = readSchema.getType();
    switch (targetType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return createStringConverter(targetType, readSchema);
      case BOOLEAN:
        return createBooleanConverter(targetType, schemaType);
      case BINARY:
      case VARBINARY:
        return createBinaryConverter(targetType, readSchema);
      case TIME_WITHOUT_TIME_ZONE:
        return createTimeConverter();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return createTimestampConverter();
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        requireAlternative(readSchema, targetType);
        // A google.protobuf.Timestamp is an instant; TIMESTAMP (no time zone) reads it as the
        // wall-clock time in UTC, which is what TimestampData.fromEpochMillis holds.
        return createTimestampConverter();
      case DATE:
        return createDateConverter();
      case INTERVAL_DAY_TIME:
        requireAlternative(readSchema, targetType);
        return createDurationConverter();
      case DECIMAL:
        return createDecimalConverter((DecimalType) targetType, readSchema);
      case TINYINT:
        return createTinyIntConverter(targetType, readSchema);
      case SMALLINT:
        return createSmallIntConverter(targetType, readSchema);
      case INTEGER:
        return createIntegerConverter(targetType, schemaType);
      case BIGINT:
        return createBigintConverter(targetType, readSchema);
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
      case DISTINCT_TYPE:
      case STRUCTURED_TYPE:
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
      LogicalType targetType, FieldDescriptor readSchema) {
    final Type schemaType = readSchema.getType();
    return switch (schemaType) {
      case STRING, ENUM -> object -> StringData.fromString(object.toString());
      case MESSAGE -> {
        if (DeclaredTypes.isSerializedMessage(readSchema)) {
          yield object -> StringData.fromString(ProtoJson.toJson((Message) object));
        }
        yield object -> StringData.fromString(extractValueField(object).toString());
      }
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
      LogicalType targetType, FieldDescriptor readSchema) {
    final Type schemaType = readSchema.getType();
    return switch (schemaType) {
      case BYTES -> object -> ((ByteString) object).toByteArray();
      case MESSAGE -> {
        if (DeclaredTypes.isSerializedMessage(readSchema)) {
          // The message's Protobuf encoding, e.g. a recursive field kept serialized.
          yield object -> ((Message) object).toByteArray();
        }
        yield object -> ((ByteString) extractValueField(object)).toByteArray();
      }
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
      LogicalType targetType, FieldDescriptor readSchema) {
    final Type schemaType = readSchema.getType();
    return switch (schemaType) {
      case INT32, SINT32, SFIXED32 -> object -> (byte) narrow(readSchema, targetType, object);
      case MESSAGE -> object -> (byte) narrow(readSchema, targetType, extractValueField(object));
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  private static ProtoToRowDataConverter createSmallIntConverter(
      LogicalType targetType, FieldDescriptor readSchema) {
    final Type schemaType = readSchema.getType();
    return switch (schemaType) {
      case INT32, SINT32, SFIXED32 -> object -> (short) narrow(readSchema, targetType, object);
      case MESSAGE -> object -> (short) narrow(readSchema, targetType, extractValueField(object));
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  /**
   * Returns a 32-bit value that fits the {@code TINYINT} or {@code SMALLINT} column, and fails the
   * record (handled per {@code on-deserialize-error}) for one that does not instead of wrapping it.
   */
  private static int narrow(FieldDescriptor readSchema, LogicalType targetType, Object object)
      throws IOException {
    final int value = ((Number) object).intValue();
    final boolean tinyInt = targetType.getTypeRoot() == LogicalTypeRoot.TINYINT;
    final int min = tinyInt ? Byte.MIN_VALUE : Short.MIN_VALUE;
    final int max = tinyInt ? Byte.MAX_VALUE : Short.MAX_VALUE;
    if (value < min || value > max) {
      throw new IOException(
          String.format(
              "Value %d of field '%s' is out of range for a %s column (%d to %d)",
              value, readSchema.getFullName(), targetType.getTypeRoot(), min, max));
    }
    return value;
  }

  private static ProtoToRowDataConverter createIntegerConverter(
      LogicalType targetType, Type schemaType) {
    return switch (schemaType) {
      case INT32, SINT32, SFIXED32 -> object -> ((Number) object).intValue();
      // The enum's number. A number the reader's enum does not define (an open enum's unknown
      // value) reads as that number too.
      case ENUM -> object -> ((EnumValueDescriptor) object).getNumber();
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
      LogicalType targetType, FieldDescriptor readSchema) {
    final Type schemaType = readSchema.getType();
    return switch (schemaType) {
      // Java holds uint32/fixed32 in a signed int: widen it without sign extension.
      case UINT32, FIXED32 -> object -> Integer.toUnsignedLong((Integer) object);
      // uint64/fixed64 values above Long.MAX_VALUE read as negative (same 64 bits); declare the
      // column DECIMAL(20, 0) to read them as unsigned.
      case INT64, UINT64, SINT64, FIXED64, SFIXED64 -> object -> ((Number) object).longValue();
      case MESSAGE -> {
        if (readSchema.getMessageType().getFullName().equals(UINT32_WRAPPER)) {
          yield object -> Integer.toUnsignedLong((Integer) extractValueField(object));
        }
        yield object -> ((Number) extractValueField(object)).longValue();
      }
      default -> throw unexpectedTypeForSchema(schemaType, targetType.getTypeRoot());
    };
  }

  /** Fails when the reader is built, not on the first record, for a declared-only type. */
  private static void requireAlternative(FieldDescriptor readSchema, LogicalType targetType) {
    if (!DeclaredTypes.isAlternative(readSchema, targetType)) {
      throw unexpectedTypeForSchema(readSchema.getType(), targetType.getTypeRoot());
    }
  }

  private static IllegalStateException unexpectedTypeForSchema(
      Type schemaType, LogicalTypeRoot flinkType) {
    return new IllegalStateException(
        String.format("Unsupported protobuf type: %s for a SQL type: %s", schemaType, flinkType));
  }

  private static ProtoToRowDataConverter createDecimalConverter(
      DecimalType targetType, FieldDescriptor readSchema) {
    final int precision = targetType.getPrecision();
    final int scale = targetType.getScale();
    if (DeclaredTypes.isAlternative(readSchema, targetType)) {
      // uint64, fixed64 or google.protobuf.UInt64Value.
      return readSchema.getType() == Type.MESSAGE
          ? object -> unsignedDecimal((Long) extractValueField(object), precision, scale)
          : object -> unsignedDecimal((Long) object, precision, scale);
    }
    return object -> {
      final ByteString valueField = (ByteString) extractValueField(object);
      return DecimalData.fromUnscaledBytes(
          valueField.toByteArray(), targetType.getPrecision(), targetType.getScale());
    };
  }

  /**
   * Reads a uint64/fixed64 value, which Java holds in a signed long, as its unsigned value. A value
   * the column's precision cannot hold reads as NULL, as Flink's casts do.
   */
  private static DecimalData unsignedDecimal(long value, int precision, int scale) {
    return DecimalData.fromBigDecimal(
        new BigDecimal(Long.toUnsignedString(value)), precision, scale);
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

  /**
   * Reads a {@code google.protobuf.Duration} as an {@code INTERVAL DAY TO SECOND}, which Flink
   * holds in milliseconds: nanoseconds below a millisecond are dropped (toward zero).
   */
  private static ProtoToRowDataConverter createDurationConverter() {
    // Only built for a google.protobuf.Duration field (see requireAlternative).
    return object -> {
      final Message message = (Message) object;
      final Descriptor duration = message.getDescriptorForType();
      final long seconds = (Long) message.getField(duration.findFieldByName("seconds"));
      final int nanos = (Integer) message.getField(duration.findFieldByName("nanos"));
      return Math.addExact(Math.multiplyExact(seconds, 1000L), nanos / 1000_000L);
    };
  }

  private static Object extractValueField(Object value) {
    final Message message = (Message) value;
    final FieldDescriptor fieldDescriptor =
        message.getDescriptorForType().findFieldByName(VALUE_FIELD);

    return message.getField(fieldDescriptor);
  }
}
