package com.bbrownsound.flink.formats.proto.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoRowDataDeserializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.SourceMessageValidator;
import com.google.protobuf.Descriptors.Descriptor;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoDecodingFormat.class);

  private final ProtoConfluentFormatConfig formatConfig;
  /** Class loader used to look up {@code message-class} while the query is planned. */
  private final ClassLoader classLoader;

  /**
   * Creates a decoding format with the given config.
   *
   * @param config format and Schema Registry configuration
   */
  public ProtoDecodingFormat(ProtoConfluentFormatConfig config) {
    this(config, Thread.currentThread().getContextClassLoader());
  }

  /**
   * Creates a decoding format with the given config.
   *
   * @param config format and Schema Registry configuration
   * @param classLoader the planner's user class loader, used to look up {@code message-class} so
   *     that a table that does not match it is rejected when the query is planned
   */
  public ProtoDecodingFormat(ProtoConfluentFormatConfig config, ClassLoader classLoader) {
    formatConfig = config;
    this.classLoader = classLoader;
  }

  @Override
  public DeserializationSchema<RowData> createRuntimeDecoder(
      DynamicTableSource.Context context, DataType producedDataType) {
    final RowType rowType = (RowType) producedDataType.getLogicalType();
    validateAgainstMessageClass(rowType);
    final TypeInformation<RowData> rowDataTypeInfo =
        context.createTypeInformation(producedDataType);
    LOG.debug(
        "[proto-confluent] createRuntimeDecoder: topic={}, rowTypeFields={}",
        formatConfig.topic,
        rowType.getFieldNames());
    return new ProtoRowDataDeserializationSchema(rowType, rowDataTypeInfo, formatConfig);
  }

  /**
   * Checks the produced row type against {@code message-class} while the query is planned. A class
   * the planner cannot load (for example one shipped only in the job JAR) is checked when the
   * source opens instead.
   */
  private void validateAgainstMessageClass(RowType rowType) {
    final String messageClass = formatConfig.getMessageClass();
    if (messageClass == null) {
      return;
    }
    final Descriptor descriptor;
    try {
      descriptor =
          SourceMessageValidator.loadDescriptor(
              messageClass,
              classLoader != null ? classLoader : ProtoDecodingFormat.class.getClassLoader());
    } catch (ClassNotFoundException | LinkageError e) {
      LOG.info(
          "[proto-confluent] message-class '{}' is not visible to the planner; the table is "
              + "checked against it when the source opens",
          messageClass);
      return;
    }
    SourceMessageValidator.expectedMessageName(formatConfig.messageName, messageClass, descriptor);
    SourceMessageValidator.validateRowType(descriptor, rowType, messageClass);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }
}
