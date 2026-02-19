package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.lang.reflect.Method;
import java.util.Set;
import org.apache.flink.table.types.logical.LogicalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper to generate a Flink table DDL schema string from a protobuf message class. Uses {@link
 * ProtoToLogicalType} and {@link LogicalTypeToDDL} so the output matches what proto-confluent
 * expects. Useful for generating or validating CREATE TABLE (...) column definitions.
 */
public final class ProtoToTableDDL {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoToTableDDL.class);

  /**
   * Returns the table schema DDL string (column list with backticked names and types) for the given
   * protobuf message class. The schema matches the mapping used by proto-confluent deserialization.
   *
   * @param messageClass a generated protobuf message class (e.g. RawObservation.class)
   * @return the DDL column list string, e.g. "`status` ROW&lt;...&gt;, `organization_domain` STRING
   *     NOT NULL, ..."
   * @throws IllegalArgumentException if the class is not a protobuf message or descriptor cannot be
   *     obtained
   */
  public static String tableSchemaFromProto(Class<? extends Message> messageClass) {
    return tableSchemaFromProto(messageClass, false);
  }

  /**
   * Same as {@link #tableSchemaFromProto(Class)} but with optional strict NOT NULL: when true,
   * TIMESTAMP_LTZ/DATE/TIME stay nullable; all other types are NOT NULL so the DDL matches
   * hand-written schemas that mark scalars and nested fields as NOT NULL.
   *
   * @param messageClass a generated protobuf message class
   * @param strictNotNull whether to apply strict NOT NULL
   * @return the DDL column list string
   */
  public static String tableSchemaFromProto(
      Class<? extends Message> messageClass, boolean strictNotNull) {
    return tableSchemaFromProto(messageClass, strictNotNull, null);
  }

  /**
   * Same as {@link #tableSchemaFromProto(Class, boolean)} but with a set of field paths that must
   * remain nullable (dot-separated; array elements use "[]", e.g.
   * "evidence.evidence.cookie.observations[].cookie_name"). Use null or empty for no extra paths.
   *
   * @param messageClass a generated protobuf message class
   * @param strictNotNull whether to apply strict NOT NULL
   * @param keepNullablePaths paths that remain nullable, or null
   * @return the DDL column list string
   */
  public static String tableSchemaFromProto(
      Class<? extends Message> messageClass, boolean strictNotNull, Set<String> keepNullablePaths) {
    Descriptor descriptor = getDescriptor(messageClass);
    LogicalType logicalType = ProtoToLogicalType.toLogicalType(descriptor);
    if (strictNotNull) {
      logicalType = LogicalTypeToDDL.toStrictNotNull(logicalType, "", keepNullablePaths);
    }
    return LogicalTypeToDDL.toTableSchemaDDL(logicalType);
  }

  /**
   * Returns the table schema DDL string for the given descriptor. Useful when you already have a
   * {@link Descriptor} (e.g. from a DynamicMessage).
   *
   * @param descriptor the Protobuf descriptor
   * @return the DDL column list string
   */
  public static String tableSchemaFromDescriptor(Descriptor descriptor) {
    return tableSchemaFromDescriptor(descriptor, false);
  }

  /**
   * Same as {@link #tableSchemaFromDescriptor(Descriptor)} but with optional strict NOT NULL (see
   * {@link #tableSchemaFromProto(Class, boolean)}).
   *
   * @param descriptor the Protobuf descriptor
   * @param strictNotNull whether to apply strict NOT NULL
   * @return the DDL column list string
   */
  public static String tableSchemaFromDescriptor(Descriptor descriptor, boolean strictNotNull) {
    return tableSchemaFromDescriptor(descriptor, strictNotNull, null);
  }

  /**
   * Same as {@link #tableSchemaFromDescriptor(Descriptor, boolean)} but with keepNullablePaths (see
   * {@link #tableSchemaFromProto(Class, boolean, Set)}).
   *
   * @param descriptor the Protobuf descriptor
   * @param strictNotNull whether to apply strict NOT NULL
   * @param keepNullablePaths paths that remain nullable, or null
   * @return the DDL column list string
   */
  public static String tableSchemaFromDescriptor(
      Descriptor descriptor, boolean strictNotNull, Set<String> keepNullablePaths) {
    LogicalType logicalType = ProtoToLogicalType.toLogicalType(descriptor);
    if (strictNotNull) {
      logicalType = LogicalTypeToDDL.toStrictNotNull(logicalType, "", keepNullablePaths);
    }
    return LogicalTypeToDDL.toTableSchemaDDL(logicalType);
  }

  /**
   * Logs the table schema DDL for the given message class at debug level. Convenience for codegen
   * or debugging.
   *
   * @param messageClass a generated protobuf message class
   */
  public static void printTableSchemaFromProto(Class<? extends Message> messageClass) {
    LOG.debug("tableSchemaFromProto: {}", tableSchemaFromProto(messageClass));
  }

  private static Descriptor getDescriptor(Class<? extends Message> messageClass) {
    try {
      Method getDescriptor = messageClass.getMethod("getDescriptor");
      Object result = getDescriptor.invoke(null);
      if (result instanceof Descriptor) {
        return (Descriptor) result;
      }
      throw new IllegalArgumentException(
          "getDescriptor() did not return a Descriptor: " + messageClass.getName());
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Not a protobuf message class (no getDescriptor()): " + messageClass.getName(), e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to get descriptor for " + messageClass.getName(), e);
    }
  }

  private ProtoToTableDDL() {}
}
