package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.REGISTRY_URL;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.ROW_DERIVED_FULL_NAME;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.SUBJECT;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.decode;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The {@code derived-message-name} option (GitHub issue #246): the full name of the message a sink
 * derives from the table when its subject has no schema, and so the subject that message registers
 * under with the {@code record} strategy.
 */
class DerivedMessageNameTest {

  private static final RowType ROW_TYPE = rowType("content", "date_time");

  private MockSchemaRegistryClient registry;

  @BeforeEach
  void setUp() {
    registry = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
  }

  /** The schema with its registry client pointed at the in-memory registry. */
  private final class StubbedSchema extends ProtoRowDataSerializationSchema {
    private static final long serialVersionUID = 1L;

    StubbedSchema(ProtoConfluentFormatConfig config) {
      super(ROW_TYPE, config);
    }

    @Override
    SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
      return registry;
    }
  }

  private static ProtoConfluentFormatConfig config(String strategy, String derivedMessageName) {
    Configuration options = new Configuration();
    options.setString("url", REGISTRY_URL);
    options.setString("topic", TOPIC);
    options.setString("auto-register-schemas", "true");
    options.setString("subject-name-strategy", strategy);
    if (derivedMessageName != null) {
      options.setString("derived-message-name", derivedMessageName);
    }
    return new ProtoConfluentFormatConfig(options);
  }

  private byte[] write(ProtoConfluentFormatConfig config) {
    ProtoRowDataSerializationSchema schema = new StubbedSchema(config);
    schema.open(null);
    return schema.serialize(row("c", "2025-09-01"));
  }

  @ParameterizedTest
  @CsvSource({
    "com.acme.orders.Order, com.acme.orders.Order",
    "Order, Order",
  })
  void recordStrategy_registersTheDerivedMessageUnderItsConfiguredName(
      String derivedMessageName, String expectedSubject) throws Exception {
    byte[] bytes = write(config("record", derivedMessageName));

    assertEquals(List.of(expectedSubject), List.copyOf(registry.getAllSubjects()));
    assertEquals(
        derivedMessageName, decode(registry, bytes).getDescriptorForType().getFullName());
  }

  @Test
  void recordStrategy_withoutTheOption_keepsTheRowName() throws Exception {
    write(config("record", null));

    assertEquals(List.of(ROW_DERIVED_FULL_NAME), List.copyOf(registry.getAllSubjects()));
  }

  @Test
  void topicStrategy_namesTheMessageRegisteredUnderTheTopicSubject() throws Exception {
    byte[] bytes = write(config("topic", "com.acme.orders.Order"));

    assertEquals(List.of(SUBJECT), List.copyOf(registry.getAllSubjects()));
    assertEquals(
        "com.acme.orders.Order", decode(registry, bytes).getDescriptorForType().getFullName());
  }

  /** A serializer built without the option, as by a config from an earlier release, keeps Row. */
  @Test
  void serializerWithoutAName_derivesTheRowMessage() throws Exception {
    RowDataProtoSerializer serializer = SinkTestSupport.serializer(registry, true);

    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("c", "d"));

    assertEquals(
        ROW_DERIVED_FULL_NAME, decode(registry, bytes).getDescriptorForType().getFullName());
  }
}
