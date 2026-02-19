package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class ProtoRowDataDeserializationSchemaTest {

  @Test
  void deserialize_nullReturnsNull() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081", "test-topic", false, Collections.emptyMap());
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    RowData result = schema.deserialize(null);
    assertNull(result);
  }

  @Test
  void isEndOfStream_returnsFalse() {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081", "test-topic", false, Collections.emptyMap());
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    assertFalse(schema.isEndOfStream(null));
  }

  @Test
  void getProducedType_returnsResultTypeInfo() {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081", "test-topic", false, Collections.emptyMap());
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    assertNull(schema.getProducedType());
  }

  @Test
  void deserialize_invalidBytesThrowsIOException() {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    Map<String, String> props = Map.of("schema.registry.url", "http://localhost:8081");
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081", "test-topic", false, props);
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    schema.open(null);
    byte[] invalid = new byte[] {0, 1, 2, 3, 4};
    assertThrows(IOException.class, () -> schema.deserialize(invalid));
  }

  @Test
  void open_initializesClientAndDeserializer() {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    Map<String, String> props =
        Map.of("schema.registry.url", "http://localhost:8081");
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081", "test-topic", false, props);
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    schema.open(null);
    schema.open(null);
    assertFalse(schema.isEndOfStream(null));
  }
}
