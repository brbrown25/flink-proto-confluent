package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;

class ProtoEncodingFormatTest {

  @Test
  void getChangelogMode() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://localhost:8081", "t", false, java.util.Map.of());
    ProtoEncodingFormat format = new ProtoEncodingFormat(config);
    assertEquals(ChangelogMode.insertOnly(), format.getChangelogMode());
  }

  @Test
  void createRuntimeEncoder_returnsSerializationSchema() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081", "test-topic", false, java.util.Map.of());
    ProtoEncodingFormat format = new ProtoEncodingFormat(config);
    DataType dataType =
        org.apache.flink.table.api.DataTypes.ROW(
            org.apache.flink.table.api.DataTypes.FIELD(
                "a", org.apache.flink.table.api.DataTypes.VARCHAR(100)),
            org.apache.flink.table.api.DataTypes.FIELD(
                "b", org.apache.flink.table.api.DataTypes.VARCHAR(100)));

    org.apache.flink.api.common.serialization.SerializationSchema<
            org.apache.flink.table.data.RowData>
        schema = format.createRuntimeEncoder((DynamicTableSink.Context) null, dataType);

    assertNotNull(schema);
    assertTrue(
        org.apache.flink.api.common.serialization.SerializationSchema.class.isAssignableFrom(
            schema.getClass()));
  }
}
