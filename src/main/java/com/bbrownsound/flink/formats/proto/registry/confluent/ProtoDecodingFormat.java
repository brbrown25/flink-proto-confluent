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

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoDecodingFormat implements DecodingFormat<DeserializationSchema<RowData>> {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoDecodingFormat.class);

  private final ProtoConfluentFormatConfig formatConfig;

  /**
   * Creates a decoding format with the given config.
   *
   * @param config format and Schema Registry configuration
   */
  public ProtoDecodingFormat(ProtoConfluentFormatConfig config) {
    formatConfig = config;
  }

  @Override
  public DeserializationSchema<RowData> createRuntimeDecoder(
      DynamicTableSource.Context context, DataType producedDataType) {
    final RowType rowType = (RowType) producedDataType.getLogicalType();
    final TypeInformation<RowData> rowDataTypeInfo =
        context.createTypeInformation(producedDataType);
    LOG.debug(
        "[proto-confluent] createRuntimeDecoder: topic={}, rowTypeFields={}",
        formatConfig.topic,
        rowType.getFieldNames());
    return new ProtoRowDataDeserializationSchema(rowType, rowDataTypeInfo, formatConfig);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }
}
