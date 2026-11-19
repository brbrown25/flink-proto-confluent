package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Source-side {@code message-class} / {@code message-name} handling (GitHub issue #177). */
class ProtoRowDataDeserializationSchemaMessageTypeTest {

  private static final String TOPIC = "orders";
  private static final String SIMPLE = "com.bbrownsound.flink.formats.proto.test.v1.SimpleMessage";
  private static final RowType SIMPLE_ROW =
      (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());

  private MockSchemaRegistryClient registry;
  private byte[] simpleRecord;
  private byte[] mapRecord;

  @BeforeEach
  void setUp() {
    registry =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    simpleRecord =
        serialize(
            "simple-topic",
            TestSimple.SimpleMessage.newBuilder().setContent("hello").setDateTime("d").build());
    mapRecord =
        serialize(
            "map-topic",
            TestMap.MapTest.newBuilder().putSimpleStringMap("content", "not-an-order").build());
  }

  private byte[] serialize(String topic, Message message) {
    try (KafkaProtobufSerializer<Message> serializer = new KafkaProtobufSerializer<>(registry)) {
      serializer.configure(
          Map.of("schema.registry.url", "mock://unused", "auto.register.schemas", "true"), false);
      return serializer.serialize(topic, message);
    }
  }

  private ProtoConfluentFormatConfig config(String messageName, String messageClass) {
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.url", "http://localhost:8081");
    if (messageClass != null) {
      props.put("value.message-class", messageClass);
    }
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://localhost:8081", TOPIC, false, props);
    config.messageName = messageName;
    return config;
  }

  private ProtoRowDataDeserializationSchema schema(
      RowType rowType, ProtoConfluentFormatConfig config) {
    return new ProtoRowDataDeserializationSchema(rowType, null, config) {
      @Override
      SchemaRegistryClient createSchemaRegistryClient(InitializationContext context) {
        return registry;
      }
    };
  }

  private static DeserializationSchema.InitializationContext contextFor(MetricListener listener) {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return listener.getMetricGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(
            ProtoRowDataDeserializationSchemaMessageTypeTest.class.getClassLoader());
      }
    };
  }

  @Test
  void withoutExpectedMessage_anyMessageIsRead() throws IOException {
    var schema = schema(SIMPLE_ROW, config(null, null));
    schema.open(null);
    assertNotNull(schema.deserialize(simpleRecord));
    // Read by column name: the map message has no 'content' or 'date_time' field.
    RowData row = schema.deserialize(mapRecord);
    assertNotNull(row);
    assertTrue(row.isNullAt(0));
  }

  @Test
  void messageName_matchingRecordIsRead() throws IOException {
    var schema = schema(SIMPLE_ROW, config(SIMPLE, null));
    schema.open(null);
    RowData row = schema.deserialize(simpleRecord);
    assertEquals("hello", row.getString(0).toString());
  }

  @Test
  void messageName_otherMessageFailsByDefault() {
    var schema = schema(SIMPLE_ROW, config(SIMPLE, null));
    schema.open(null);
    IOException e = assertThrows(IOException.class, () -> schema.deserialize(mapRecord));
    UnexpectedMessageTypeException cause =
        assertInstanceOf(UnexpectedMessageTypeException.class, e.getCause());
    assertTrue(cause.getMessage().contains("'orders'"), cause.getMessage());
    assertTrue(
        cause.getMessage().contains("'com.bbrownsound.flink.formats.proto.test.v1.MapTest'"),
        cause.getMessage());
    assertTrue(cause.getMessage().contains("expects '" + SIMPLE + "'"), cause.getMessage());
  }

  @Test
  void messageName_otherMessageWithoutATopic_namesNoTopic() {
    // GitHub issue #212: a source with 'subject' and no 'topic' (for example a 'topic-pattern'
    // table) does not know the record's topic, so the error must not claim the topic is 'null'.
    ProtoConfluentFormatConfig config = config(SIMPLE, null);
    config.topic = null;
    config.subject = "orders-value";
    var schema = schema(SIMPLE_ROW, config);
    schema.open(null);
    IOException e = assertThrows(IOException.class, () -> schema.deserialize(mapRecord));
    UnexpectedMessageTypeException cause =
        assertInstanceOf(UnexpectedMessageTypeException.class, e.getCause());
    assertTrue(
        cause
            .getMessage()
            .startsWith("record is a 'com.bbrownsound.flink.formats.proto.test.v1.MapTest'"),
        cause.getMessage());
    assertFalse(cause.getMessage().contains("null"), cause.getMessage());
  }

  @Test
  void messageName_otherMessageIsSkippedAndCountedWithSkip() throws IOException {
    ProtoConfluentFormatConfig config = config(SIMPLE, null);
    config.onDeserializeError = OnDeserializeError.SKIP;
    var schema = schema(SIMPLE_ROW, config);
    MetricListener listener = new MetricListener();
    schema.open(contextFor(listener));

    assertNull(schema.deserialize(mapRecord));
    assertNotNull(schema.deserialize(simpleRecord));
    assertNull(schema.deserialize(mapRecord));
    assertEquals(2L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void messageClass_setsTheExpectedMessage() throws IOException {
    var schema = schema(SIMPLE_ROW, config(null, TestSimple.SimpleMessage.class.getName()));
    schema.open(contextFor(new MetricListener()));
    assertNotNull(schema.deserialize(simpleRecord));
    IOException e = assertThrows(IOException.class, () -> schema.deserialize(mapRecord));
    assertInstanceOf(UnexpectedMessageTypeException.class, e.getCause());
  }

  @Test
  void messageClass_tableThatDoesNotMatchFailsAtOpen() {
    RowType rowType =
        (RowType)
            DataTypes.ROW(
                    DataTypes.FIELD("content", DataTypes.STRING()),
                    DataTypes.FIELD("order_id", DataTypes.STRING()))
                .getLogicalType();
    var schema = schema(rowType, config(null, TestSimple.SimpleMessage.class.getName()));
    ValidationException e = assertThrows(ValidationException.class, () -> schema.open(null));
    assertTrue(e.getMessage().contains("column 'order_id'"), e.getMessage());
  }

  @Test
  void messageClass_notOnTheClasspathFailsAtOpen() {
    var schema = schema(SIMPLE_ROW, config(null, "com.example.Missing$Order"));
    ValidationException e =
        assertThrows(ValidationException.class, () -> schema.open(contextFor(new MetricListener())));
    assertTrue(e.getMessage().contains("'com.example.Missing$Order'"), e.getMessage());
    assertTrue(e.getMessage().contains("not on the classpath"), e.getMessage());
  }

  @Test
  void messageClass_conflictingMessageNameFailsAtOpen() {
    var schema =
        schema(SIMPLE_ROW, config("com.example.Order", TestSimple.SimpleMessage.class.getName()));
    ValidationException e = assertThrows(ValidationException.class, () -> schema.open(null));
    assertTrue(e.getMessage().contains("does not match 'message-class'"), e.getMessage());
  }
}
