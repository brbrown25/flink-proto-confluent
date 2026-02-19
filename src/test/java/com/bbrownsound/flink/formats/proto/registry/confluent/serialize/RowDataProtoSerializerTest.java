package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

/**
 * Unit tests for {@link RowDataProtoSerializer}. Uses {@link MockSchemaRegistryClient} so no real
 * Schema Registry is needed. When a subject has no schema, getDescriptor falls back to
 * RowTypeToProto.fromRowType; the parent then registers the schema with the mock client.
 */
class RowDataProtoSerializerTest {

  private MockSchemaRegistryClient schemaRegistryClient;
  private RowDataProtoSerializer serializer;
  private RowType rowType;

  @BeforeEach
  void setUp() {
    schemaRegistryClient =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    serializer = new RowDataProtoSerializer(schemaRegistryClient);
    ProtoConfluentFormatConfig formatConfig =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081",
            "test-topic",
            false,
            Map.of("schema.registry.url", "http://localhost:8081"));
    serializer.configure(formatConfig.getProperties(), formatConfig.isKey);

    LogicalType type = ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    rowType = (RowType) type;
  }

  @Test
  void serializeRowData_whenSubjectNotRegistered_usesFallbackAndReturnsBytes() throws Exception {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("x"));
    row.setField(1, StringData.fromString("y"));

    byte[] result = serializer.serializeRowData("test-topic", rowType, row);

    assertNotNull(result);
    assertTrue(result.length > 0);
  }

  @Test
  void serializeRowData_whenSchemaCached_returnsBytes() throws Exception {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("first"));
    row.setField(1, StringData.fromString("second"));

    byte[] first = serializer.serializeRowData("cached-topic", rowType, row);
    assertNotNull(first);
    assertTrue(first.length > 0);

    byte[] second = serializer.serializeRowData("cached-topic", rowType, row);
    assertNotNull(second);
    assertTrue(second.length > 0);
  }

  @Test
  void serializeRowData_whenSubjectRegistered_usesRegistrySchema() throws Exception {
    String topic = "registry-topic";
    String subject = topic + "-value";
    ProtobufSchema schema = new ProtobufSchema(TestSimple.SimpleMessage.getDescriptor());
    schemaRegistryClient.register(subject, schema);

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("a"));
    row.setField(1, StringData.fromString("b"));

    byte[] result = serializer.serializeRowData(topic, rowType, row);
    assertNotNull(result);
    assertTrue(result.length > 0);
  }
}
