package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.DeclaredTypes;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Checks a source table against the message it expects to read: the message named by {@code
 * message-class} (whose descriptor is checked column by column) and/or {@code message-name} (which
 * every record's writer message must carry).
 *
 * <p>The column check mirrors how {@link ProtoToRowDataConverters} reads a record: columns are
 * matched to fields by name (exact, then {@code snake_case} to {@code camelCase}), a column named
 * after a {@code oneof} reads that oneof as a {@code ROW} whose columns are matched to the oneof's
 * cases by position, and each column's type must be one the converter can produce from the field.
 * Every table column must exist in the message; message fields the table does not declare are
 * ignored.
 */
public final class SourceMessageValidator {

  /** A fully-qualified Protobuf message name: dot-separated identifiers, no leading dot. */
  private static final Pattern FULL_NAME =
      Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*");

  private static final String MAP_KEY_FIELD = "key";
  private static final String MAP_VALUE_FIELD = "value";

  private SourceMessageValidator() {}

  /**
   * Checks that a {@code message-name} value is a fully-qualified Protobuf message name such as
   * {@code com.example.Order}.
   *
   * @param optionKey the option the value came from, for the error message
   * @param messageName the configured value
   * @throws ValidationException if the value is not a dot-separated list of identifiers
   */
  public static void validateMessageName(String optionKey, String messageName) {
    if (messageName == null || !FULL_NAME.matcher(messageName).matches()) {
      throw new ValidationException(
          "'"
              + optionKey
              + "' must be a fully-qualified protobuf message name such as 'com.example.Order' "
              + "(the proto package followed by the message name, nested messages separated by "
              + "'.'), got '"
              + messageName
              + "'.");
    }
  }

  /**
   * Loads a generated Protobuf message class and returns its descriptor.
   *
   * @param className the JVM binary name of the class (nested classes use {@code $})
   * @param classLoader the class loader to load it with
   * @return the descriptor of the message the class represents
   * @throws ClassNotFoundException if the class loader cannot find the class
   * @throws ValidationException if the class is not a generated Protobuf message class
   */
  public static Descriptor loadDescriptor(String className, ClassLoader classLoader)
      throws ClassNotFoundException {
    final Class<?> clazz = Class.forName(className, true, classLoader);
    if (!Message.class.isAssignableFrom(clazz)) {
      throw new ValidationException(
          "'message-class' = '" + className + "' is not a generated protobuf message class.");
    }
    try {
      final Method getDefaultInstance = clazz.getMethod("getDefaultInstance");
      return ((Message) getDefaultInstance.invoke(null)).getDescriptorForType();
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new ValidationException(
          "Failed to read the descriptor of 'message-class' = '" + className + "'.", e);
    }
  }

  /**
   * Returns the full name every record must carry: {@code message-name} when it is set, otherwise
   * the full name of the {@code message-class} descriptor, otherwise {@code null} (no check).
   *
   * @param messageName the {@code message-name} option, or {@code null}
   * @param messageClass the {@code message-class} option, or {@code null}
   * @param descriptor the descriptor of {@code message-class}, or {@code null} if it is unset
   * @return the expected message full name, or {@code null}
   * @throws ValidationException if both options are set and name different messages
   */
  public static @Nullable String expectedMessageName(
      @Nullable String messageName, @Nullable String messageClass, @Nullable Descriptor descriptor) {
    if (descriptor == null) {
      return messageName;
    }
    if (messageName != null && !messageName.equals(descriptor.getFullName())) {
      throw new ValidationException(
          "'message-name' = '"
              + messageName
              + "' does not match 'message-class' = '"
              + messageClass
              + "', which is the protobuf message '"
              + descriptor.getFullName()
              + "'. Set one of them, or make them name the same message.");
    }
    return descriptor.getFullName();
  }

  /**
   * Checks that every column of the table's row type can be read from the message.
   *
   * @param descriptor the expected message
   * @param rowType the row type the source produces
   * @param messageClass the {@code message-class} value the descriptor came from, for the error
   * @throws ValidationException naming each offending column if the table does not match
   */
  public static void validateRowType(Descriptor descriptor, RowType rowType, String messageClass) {
    final List<String> problems = new ArrayList<>();
    checkRow(descriptor, rowType, "", problems);
    if (!problems.isEmpty()) {
      throw new ValidationException(
          "The table does not match 'message-class' = '"
              + messageClass
              + "' (protobuf message '"
              + descriptor.getFullName()
              + "'): "
              + String.join("; ", problems)
              + ". Fix the column(s), or remove 'message-class' to read the topic by column name "
              + "only.");
    }
  }

  private static void checkRow(
      Descriptor descriptor, RowType rowType, String path, List<String> problems) {
    final Map<String, OneofDescriptor> oneofs =
        descriptor.getRealOneofs().stream()
            .collect(Collectors.toMap(OneofDescriptor::getName, Function.identity()));
    // Fields inside a oneof are reached through the oneof's column, not by their own name.
    final Map<String, FieldDescriptor> fields =
        descriptor.getFields().stream()
            .filter(f -> f.getRealContainingOneof() == null)
            .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));
    for (RowType.RowField column : rowType.getFields()) {
      final String columnPath = child(path, column.getName());
      final OneofDescriptor oneof = oneofs.get(column.getName());
      if (oneof != null) {
        checkOneof(oneof, column.getType(), columnPath, problems);
        continue;
      }
      final FieldDescriptor field =
          ProtoToRowDataConverters.findFieldDescriptor(fields, column.getName());
      if (field == null) {
        problems.add(
            "column '"
                + columnPath
                + "' has no field in message '"
                + descriptor.getFullName()
                + "' (fields: "
                + String.join(", ", fieldNames(descriptor))
                + ")");
        continue;
      }
      checkField(field, column.getType(), columnPath, problems);
    }
  }

  private static List<String> fieldNames(Descriptor descriptor) {
    final List<String> names = new ArrayList<>();
    descriptor.getRealOneofs().forEach(o -> names.add(o.getName()));
    descriptor.getFields().stream()
        .filter(f -> f.getRealContainingOneof() == null)
        .forEach(f -> names.add(f.getName()));
    return names;
  }

  private static void checkOneof(
      OneofDescriptor oneof, LogicalType type, String path, List<String> problems) {
    if (!(type instanceof RowType)) {
      problems.add(
          "column '"
              + path
              + "' is "
              + type.asSummaryString()
              + " but oneof '"
              + oneof.getFullName()
              + "' reads as a ROW of its cases");
      return;
    }
    // The converter maps a oneof's columns to its cases by position.
    final RowType rowType = (RowType) type;
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      final String columnPath = child(path, rowType.getFieldNames().get(i));
      if (i >= oneof.getFieldCount()) {
        problems.add(
            "column '"
                + columnPath
                + "' has no case in oneof '"
                + oneof.getFullName()
                + "', which has "
                + oneof.getFieldCount()
                + " case(s)");
        continue;
      }
      checkField(oneof.getField(i), rowType.getTypeAt(i), columnPath, problems);
    }
  }

  private static void checkField(
      FieldDescriptor field, LogicalType type, String path, List<String> problems) {
    if (field.isMapField()) {
      checkMap(field, type, path, problems);
    } else if (field.isRepeated()) {
      if (type instanceof ArrayType) {
        checkValue(field, ((ArrayType) type).getElementType(), path + "[]", problems);
      } else {
        problems.add(mismatch(path, type, field, "an ARRAY"));
      }
    } else {
      checkValue(field, type, path, problems);
    }
  }

  private static void checkMap(
      FieldDescriptor field, LogicalType type, String path, List<String> problems) {
    final Descriptor entry = field.getMessageType();
    final FieldDescriptor keyField = entry.findFieldByName(MAP_KEY_FIELD);
    final FieldDescriptor valueField = entry.findFieldByName(MAP_VALUE_FIELD);
    if (type instanceof MapType) {
      final MapType mapType = (MapType) type;
      checkValue(keyField, mapType.getKeyType(), path + ".key", problems);
      checkField(valueField, mapType.getValueType(), path + ".value", problems);
    } else if (type instanceof MultisetType) {
      checkValue(keyField, ((MultisetType) type).getElementType(), path + ".key", problems);
    } else {
      problems.add(mismatch(path, type, field, "a MAP"));
    }
  }

  /** Checks one value of the field (an element, for a repeated field) against the column type. */
  private static void checkValue(
      FieldDescriptor field, LogicalType type, String path, List<String> problems) {
    if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
        && type.getTypeRoot() == LogicalTypeRoot.ROW) {
      checkRow(field.getMessageType(), (RowType) type, path, problems);
      return;
    }
    if (DeclaredTypes.isAlternative(field, type)) {
      return;
    }
    final LogicalType expected;
    try {
      expected = ProtoToLogicalType.toValueLogicalType(field);
    } catch (IllegalArgumentException cyclic) {
      problems.add(mismatch(path, type, field, "a ROW"));
      return;
    }
    if (family(expected.getTypeRoot()) != family(type.getTypeRoot())) {
      problems.add(mismatch(path, type, field, expected.asSummaryString()));
    }
  }

  private static String mismatch(
      String path, LogicalType type, FieldDescriptor field, String readsAs) {
    final String protoType =
        field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
            ? field.getMessageType().getFullName()
            : field.getType().name().toLowerCase(Locale.ROOT);
    return "column '"
        + path
        + "' is "
        + type.asSummaryString()
        + " but field '"
        + field.getFullName()
        + "' ("
        + (field.isRepeated() ? "repeated " : "")
        + protoType
        + ") reads as "
        + readsAs;
  }

  /**
   * Groups the type roots the converters read interchangeably: any 32-bit integer field reads into
   * {@code TINYINT}, {@code SMALLINT} or {@code INT}, and length/precision variants of strings and
   * bytes are equivalent.
   */
  private static LogicalTypeRoot family(LogicalTypeRoot root) {
    switch (root) {
      case CHAR:
        return LogicalTypeRoot.VARCHAR;
      case BINARY:
        return LogicalTypeRoot.VARBINARY;
      case TINYINT:
      case SMALLINT:
        return LogicalTypeRoot.INTEGER;
      default:
        return root;
    }
  }

  private static String child(String path, String name) {
    return path.isEmpty() ? name : path + "." + name;
  }
}
