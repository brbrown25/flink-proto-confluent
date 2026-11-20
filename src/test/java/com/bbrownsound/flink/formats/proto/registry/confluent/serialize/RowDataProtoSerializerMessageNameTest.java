package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.REGISTRY_URL;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.SUBJECT;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.decode;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.field;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.schemaIdOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink with {@code message-name} writes that message of the subject's schema, not the first one
 * the schema defines (issue #244).
 */
class RowDataProtoSerializerMessageNameTest {

  /** A schema whose first message is not the one the topic carries. */
  private static final String MULTI_MESSAGE_SCHEMA =
      String.join(
          "\n",
          "syntax = \"proto3\";",
          "package com.acme.orders;",
          "message Header {",
          "  int64 sequence = 1;",
          "}",
          "message Order {",
          "  string content = 1;",
          "  string date_time = 2;",
          "}");

  private static final String ORDER = "com.acme.orders.Order";
  private static final RowType ROW_TYPE = rowType("content", "date_time");

  private StubSchemaRegistryClient registry;

  @BeforeEach
  void setUp() {
    registry = new StubSchemaRegistryClient();
  }

  private static RowDataProtoSerializer serializer(
      SchemaRegistryClient client, String messageName, Map<String, String> extraConfig) {
    RowDataProtoSerializer serializer =
        new RowDataProtoSerializer(client, null, System::nanoTime, null, messageName);
    Map<String, String> config = new HashMap<>();
    config.put("schema.registry.url", REGISTRY_URL);
    config.put("auto.register.schemas", "false");
    config.putAll(extraConfig);
    serializer.configure(config, false);
    return serializer;
  }

  @Test
  void messageName_writesThatMessageOfTheRegisteredSchema() throws Exception {
    int registeredId = registry.seed(SUBJECT, new ProtobufSchema(MULTI_MESSAGE_SCHEMA));
    RowDataProtoSerializer serializer = serializer(registry, ORDER, Map.of());

    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("hello", "2026-01-01"));

    assertEquals(registeredId, schemaIdOf(bytes));
    DynamicMessage decoded = decode(registry, bytes);
    assertEquals(ORDER, decoded.getDescriptorForType().getFullName());
    assertEquals("hello", field(decoded, "content"));
    assertEquals("2026-01-01", field(decoded, "date_time"));
    assertEquals(0, registry.registerCalls(), "nothing is registered");
    assertEquals(List.of(1), registry.versions(SUBJECT), "no version is added");
  }

  @Test
  void messageName_notInTheSchema_failsTheFirstWriteNamingTheMessagesItHas() throws Exception {
    registry.seed(SUBJECT, new ProtobufSchema(MULTI_MESSAGE_SCHEMA));
    RowDataProtoSerializer serializer = serializer(registry, "com.acme.orders.Refund", Map.of());

    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("hello", "now")));

    assertTrue(e.getMessage().contains("'com.acme.orders.Refund'"), e.getMessage());
    assertTrue(e.getMessage().contains("subject '" + SUBJECT + "'"), e.getMessage());
    assertTrue(
        e.getMessage().contains("com.acme.orders.Header, com.acme.orders.Order"), e.getMessage());
    assertEquals(0, registry.registerCalls());
  }

  @Test
  void messageName_droppedByANewerVersion_keepsWritingTheCachedSchema() throws Exception {
    int v1Id = registry.seed(SUBJECT, new ProtobufSchema(MULTI_MESSAGE_SCHEMA));
    AtomicLong clock = new AtomicLong();
    Duration ttl = Duration.ofMinutes(1);
    RowDataProtoSerializer serializer = new RowDataProtoSerializer(registry, ttl, clock::get, null, ORDER);
    serializer.configure(
        Map.of("schema.registry.url", REGISTRY_URL, "auto.register.schemas", "false"), false);
    assertEquals(v1Id, schemaIdOf(serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b"))));

    registry.seed(
        SUBJECT,
        new ProtobufSchema("syntax = \"proto3\";\npackage com.acme.orders;\nmessage Header {}\n"));
    clock.addAndGet(ttl.toNanos());

    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("still", "here"));
    assertEquals(v1Id, schemaIdOf(bytes));
    assertEquals(ORDER, decode(registry, bytes).getDescriptorForType().getFullName());
  }

  @Test
  void messageName_conflictingWithMessageClass_isRejectedOnConfigure() {
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                serializer(
                    registry,
                    ORDER,
                    Map.of(
                        RowDataProtoSerializer.VALUE_MESSAGE_CLASS_CONFIG,
                        TestSimple.SimpleMessage.class.getName())));

    assertTrue(e.getMessage().contains("'message-name' = '" + ORDER + "'"), e.getMessage());
  }

  @Test
  void messageName_matchingMessageClass_writesTheMessageClass() throws Exception {
    String simple = TestSimple.SimpleMessage.getDescriptor().getFullName();
    RowDataProtoSerializer serializer =
        serializer(
            registry,
            simple,
            Map.of(
                "auto.register.schemas",
                "true",
                RowDataProtoSerializer.VALUE_MESSAGE_CLASS_CONFIG,
                TestSimple.SimpleMessage.class.getName()));

    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("hello", "now"));

    assertEquals(simple, decode(registry, bytes).getDescriptorForType().getFullName());
  }

  @Test
  void serializationSchema_passesMessageNameToTheSerializer() throws Exception {
    int registeredId = registry.seed(SUBJECT, new ProtobufSchema(MULTI_MESSAGE_SCHEMA));
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, REGISTRY_URL);
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    options.set(ProtoConfluentFormatOptions.MESSAGE_NAME, ORDER);
    StubSchemaRegistryClient client = registry;
    ProtoRowDataSerializationSchema schema =
        new ProtoRowDataSerializationSchema(ROW_TYPE, new ProtoConfluentFormatConfig(options)) {
          private static final long serialVersionUID = 1L;

          @Override
          SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
            return client;
          }
        };
    schema.open(null);

    byte[] bytes = schema.serialize(row("hello", "now"));

    assertEquals(registeredId, schemaIdOf(bytes));
    assertEquals(ORDER, decode(registry, bytes).getDescriptorForType().getFullName());
  }
}
