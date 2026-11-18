package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Issue #176: with the {@code subject} option the sink looks up, and registers under, that subject
 * instead of one derived from the topic, and needs no topic at all.
 */
class RowDataProtoSerializerExplicitSubjectTest {

  private static final String SUBJECT = "orders-custom";

  private MockSchemaRegistryClient registry;
  private RowType rowType;
  private GenericRowData row;

  @BeforeEach
  void setUp() {
    registry = new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    rowType = (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    row = new GenericRowData(2);
    row.setField(0, StringData.fromString("content"));
    row.setField(1, StringData.fromString("2025-01-01"));
  }

  private RowDataProtoSerializer serializer(boolean isKey, boolean autoRegister) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.SUBJECT, SUBJECT);
    options.set(ProtoConfluentFormatOptions.IS_KEY, isKey);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, autoRegister);
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    RowDataProtoSerializer serializer = new RowDataProtoSerializer(registry);
    serializer.configure(config.getProperties(), config.isKey);
    return serializer;
  }

  private static int schemaId(byte[] payload) {
    return ByteBuffer.wrap(payload, 1, 4).getInt();
  }

  @Test
  void valueSink_usesTheLatestSchemaUnderTheSubject_withoutATopic() throws Exception {
    ProtobufSchema registered = new ProtobufSchema(TestSimple.SimpleMessage.getDescriptor());
    int id = registry.register(SUBJECT, registered);

    byte[] payload = serializer(false, false).serializeRowData(null, rowType, row);

    assertEquals(id, schemaId(payload));
    assertEquals(List.of(SUBJECT), List.copyOf(registry.getAllSubjects()));
  }

  @Test
  void keySink_usesTheLatestSchemaUnderTheSubject() throws Exception {
    int id = registry.register(SUBJECT, new ProtobufSchema(TestSimple.SimpleMessage.getDescriptor()));

    byte[] payload = serializer(true, false).serializeRowData("orders", rowType, row);

    assertEquals(id, schemaId(payload));
    assertEquals(List.of(SUBJECT), List.copyOf(registry.getAllSubjects()));
  }

  @Test
  void autoRegister_registersUnderTheSubject() throws Exception {
    byte[] payload = serializer(false, true).serializeRowData("orders", rowType, row);

    assertEquals(List.of(SUBJECT), List.copyOf(registry.getAllSubjects()));
    assertEquals(registry.getLatestSchemaMetadata(SUBJECT).getId(), schemaId(payload));
  }
}
