package com.bbrownsound.flink.formats.proto.registry.confluent;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.ProtoRowDataSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoEncodingFormat implements EncodingFormat<SerializationSchema<RowData>> {
  private final ProtoConfluentFormatConfig formatConfig;

  /**
   * Creates an encoding format with the given config.
   *
   * @param config format and Schema Registry configuration
   */
  public ProtoEncodingFormat(ProtoConfluentFormatConfig config) {
    formatConfig = config;
  }

  @Override
  public SerializationSchema<RowData> createRuntimeEncoder(
      DynamicTableSink.Context context, DataType physicalDataType) {
    final RowType rowType = (RowType) physicalDataType.getLogicalType();

    return new ProtoRowDataSerializationSchema(rowType, formatConfig);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }
}
