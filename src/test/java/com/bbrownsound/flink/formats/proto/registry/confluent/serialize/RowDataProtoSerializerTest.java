package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.flink.table.api.ValidationException;
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

  @Test
  void serializeRowData_whenValueMessageClassConfigured_usesMessageClassDescriptor()
      throws Exception {
    MockSchemaRegistryClient mockRegistry =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    RowDataProtoSerializer serializerWithMessageClass = new RowDataProtoSerializer(mockRegistry);
    ProtoConfluentFormatConfig formatConfig =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081",
            "test-topic",
            false,
            Map.of(
                "schema.registry.url",
                "http://localhost:8081",
                RowDataProtoSerializer.VALUE_MESSAGE_CLASS_CONFIG,
                TestSimple.SimpleMessage.class.getName()));
    serializerWithMessageClass.configure(formatConfig.getProperties(), formatConfig.isKey);

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("msg-class-content"));
    row.setField(1, StringData.fromString("2025-06-01"));

    byte[] result =
        serializerWithMessageClass.serializeRowData("test-topic", rowType, row);
    assertNotNull(result);
    assertTrue(result.length > 0);
    // The explicit value message class registers the SimpleMessage schema under the -value subject.
    ProtobufSchema registered =
        (ProtobufSchema)
            mockRegistry.getSchemaById(
                mockRegistry.getLatestSchemaMetadata("test-topic-value").getId());
    assertTrue(registered.toDescriptor().getFullName().endsWith("SimpleMessage"));
  }

  @Test
  void serializeRowData_whenIsKey_registersUnderKeySubjectAndReturnsBytes() throws Exception {
    MockSchemaRegistryClient keyRegistry =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    RowDataProtoSerializer keySerializer = new RowDataProtoSerializer(keyRegistry);
    // isKey=true: a strongly typed key must serialize and register under the -key subject.
    keySerializer.configure(
        Map.of("schema.registry.url", "http://localhost:8081", "auto.register.schemas", "true"),
        true);

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("key-content"));
    row.setField(1, StringData.fromString("2025-07-10"));

    byte[] result = keySerializer.serializeRowData("keyed-topic", rowType, row);
    assertNotNull(result);
    assertTrue(result.length > 0);
    assertNotNull(keyRegistry.getLatestSchemaMetadata("keyed-topic-key"));
  }

  @Test
  void serializeRowData_whenKeyMessageClassConfigured_usesMessageClassDescriptor()
      throws Exception {
    MockSchemaRegistryClient keyRegistry =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    RowDataProtoSerializer keySerializer = new RowDataProtoSerializer(keyRegistry);
    keySerializer.configure(
        Map.of(
            "schema.registry.url",
            "http://localhost:8081",
            "auto.register.schemas",
            "true",
            RowDataProtoSerializer.KEY_MESSAGE_CLASS_CONFIG,
            TestSimple.SimpleMessage.class.getName()),
        true);

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("typed-key"));
    row.setField(1, StringData.fromString("2025-07-10"));

    byte[] result = keySerializer.serializeRowData("keyed-topic", rowType, row);
    assertNotNull(result);
    assertTrue(result.length > 0);
    // The explicit key message class registers the SimpleMessage schema under the -key subject.
    ProtobufSchema registered =
        (ProtobufSchema)
            keyRegistry.getSchemaById(
                keyRegistry.getLatestSchemaMetadata("keyed-topic-key").getId());
    assertTrue(registered.toDescriptor().getFullName().endsWith("SimpleMessage"));
  }

  @Test
  void configure_whenKeyMessageClassOnlyApplies_whenIsKey() throws Exception {
    // value.message-class must be ignored for a key serializer; only key.message-class applies.
    MockSchemaRegistryClient keyRegistry =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    RowDataProtoSerializer keySerializer = new RowDataProtoSerializer(keyRegistry);
    keySerializer.configure(
        Map.of(
            "schema.registry.url",
            "http://localhost:8081",
            "auto.register.schemas",
            "true",
            // Deliberately set only the VALUE key with a bogus class: a key serializer must not
            // read it, so configure() must not throw and serialization falls back to dynamic.
            RowDataProtoSerializer.VALUE_MESSAGE_CLASS_CONFIG,
            "not.a.real.Class"),
        true);

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("a"));
    row.setField(1, StringData.fromString("b"));
    byte[] result = keySerializer.serializeRowData("keyed-topic", rowType, row);
    assertNotNull(result);
    assertTrue(result.length > 0);
  }

  @Test
  void configure_whenKeyMessageClassInvalid_throwsValidationException() {
    RowDataProtoSerializer badSerializer = new RowDataProtoSerializer(schemaRegistryClient);
    Map<String, Object> configs =
        Map.of(
            "schema.registry.url",
            "http://localhost:8081",
            RowDataProtoSerializer.KEY_MESSAGE_CLASS_CONFIG,
            "com.example.DoesNotExist");
    assertThrows(ValidationException.class, () -> badSerializer.configure(configs, true));
  }

  @Test
  void configure_whenValueMessageClassInvalid_throwsValidationException() {
    RowDataProtoSerializer badSerializer = new RowDataProtoSerializer(schemaRegistryClient);
    Map<String, Object> configs =
        Map.of(
            "schema.registry.url",
            "http://localhost:8081",
            RowDataProtoSerializer.VALUE_MESSAGE_CLASS_CONFIG,
            "com.example.DoesNotExist");
    assertThrows(ValidationException.class, () -> badSerializer.configure(configs, false));
  }
}
