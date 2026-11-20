package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.junit.jupiter.api.Test;

/**
 * A {@code TINYINT} or {@code SMALLINT} column on a 32-bit integer field reads values in the
 * column's range and fails the record for any other value instead of wrapping it (GitHub issue
 * #235).
 */
class NarrowIntegerRangeTest {

  private static final Descriptor NARROW =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package narrow;",
                      "import \"google/protobuf/wrappers.proto\";",
                      "message Narrow {",
                      "  int32 i32 = 1;",
                      "  sint32 s32 = 2;",
                      "  sfixed32 sf32 = 3;",
                      "  google.protobuf.Int32Value wrapped = 4;",
                      "  repeated int32 list = 5;",
                      "}"))
              .toDescriptor();

  private static final List<String> FIELDS = List.of("i32", "s32", "sf32", "wrapped");

  // ---- converter -----------------------------------------------------------------------------

  @Test
  void tinyIntReadsItsRange() throws Exception {
    for (String name : FIELDS) {
      for (int value : new int[] {Byte.MIN_VALUE, -1, 0, 1, Byte.MAX_VALUE}) {
        RowData row = read(row(name, new TinyIntType()), narrow(name, value));
        assertEquals((byte) value, row.getByte(0), name + " = " + value);
      }
    }
  }

  @Test
  void smallIntReadsItsRange() throws Exception {
    for (String name : FIELDS) {
      for (int value : new int[] {Short.MIN_VALUE, -1, 0, 1, Short.MAX_VALUE}) {
        RowData row = read(row(name, new SmallIntType()), narrow(name, value));
        assertEquals((short) value, row.getShort(0), name + " = " + value);
      }
    }
  }

  @Test
  void tinyIntFailsOutsideItsRange() {
    for (String name : FIELDS) {
      for (int value : new int[] {Byte.MIN_VALUE - 1, Byte.MAX_VALUE + 1, 300, Integer.MAX_VALUE}) {
        IOException e =
            assertThrows(
                IOException.class,
                () -> read(row(name, new TinyIntType()), narrow(name, value)),
                name + " = " + value);
        assertOutOfRange(e, name, value, "TINYINT", "-128 to 127");
      }
    }
  }

  @Test
  void smallIntFailsOutsideItsRange() {
    for (String name : FIELDS) {
      for (int value :
          new int[] {Short.MIN_VALUE - 1, Short.MAX_VALUE + 1, 100_000, Integer.MIN_VALUE}) {
        IOException e =
            assertThrows(
                IOException.class,
                () -> read(row(name, new SmallIntType()), narrow(name, value)),
                name + " = " + value);
        assertOutOfRange(e, name, value, "SMALLINT", "-32768 to 32767");
      }
    }
  }

  @Test
  void arrayElementOutsideTheRangeFails() {
    Message message =
        DynamicMessage.newBuilder(NARROW)
            .addRepeatedField(NARROW.findFieldByName("list"), 1)
            .addRepeatedField(NARROW.findFieldByName("list"), 128)
            .build();
    IOException e =
        assertThrows(
            IOException.class,
            () -> read(row("list", new ArrayType(new TinyIntType())), message));
    assertOutOfRange(e, "list", 128, "TINYINT", "-128 to 127");
  }

  // ---- full deserialization schema (on-deserialize-error) ------------------------------------

  @Test
  void outOfRangeValueFailsTheRecordByDefault() throws IOException {
    Registry registry = new Registry();
    var schema = registry.schema(OnDeserializeError.FAIL);
    schema.open(null);

    assertEquals((byte) 127, schema.deserialize(registry.record(127)).getByte(0));
    IOException e = assertThrows(IOException.class, () -> schema.deserialize(registry.record(200)));
    assertOutOfRange(e, "i32", 200, "TINYINT", "-128 to 127");
  }

  @Test
  void outOfRangeValueIsSkippedWithSkip() throws IOException {
    Registry registry = new Registry();
    var schema = registry.schema(OnDeserializeError.SKIP);
    schema.open(null);

    assertNull(schema.deserialize(registry.record(200)));
    RowData row = schema.deserialize(registry.record(-128));
    assertNotNull(row);
    assertEquals((byte) -128, row.getByte(0));
  }

  // ---- helpers -------------------------------------------------------------------------------

  private static void assertOutOfRange(
      Exception e, String field, int value, String type, String range) {
    String message = e.getMessage();
    assertTrue(message.contains("'narrow.Narrow." + field + "'"), message);
    assertTrue(message.contains(String.valueOf(value)), message);
    assertTrue(message.contains(type), message);
    assertTrue(message.contains(range), message);
  }

  private static RowType row(String name, LogicalType type) {
    return new RowType(List.of(new RowField(name, type)));
  }

  private static Message narrow(String name, int value) {
    FieldDescriptor field = NARROW.findFieldByName(name);
    Object protoValue =
        field.getType() == FieldDescriptor.Type.MESSAGE
            ? DynamicMessage.newBuilder(field.getMessageType())
                .setField(field.getMessageType().findFieldByName("value"), value)
                .build()
            : value;
    return DynamicMessage.newBuilder(NARROW).setField(field, protoValue).build();
  }

  private static RowData read(RowType type, Message message) throws Exception {
    DynamicMessage parsed = DynamicMessage.parseFrom(NARROW, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(NARROW, type).convert(parsed);
  }

  /** An in-memory registry holding the {@code Narrow} schema and records written to it. */
  private static final class Registry {
    private static final String TOPIC = "narrow";

    final MockSchemaRegistryClient client =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));

    byte[] record(int i32) {
      try (KafkaProtobufSerializer<Message> serializer = new KafkaProtobufSerializer<>(client)) {
        serializer.configure(
            Map.of("schema.registry.url", "mock://unused", "auto.register.schemas", "true"),
            false);
        return serializer.serialize(TOPIC, narrow("i32", i32));
      }
    }

    ProtoRowDataDeserializationSchema schema(OnDeserializeError onError) {
      Map<String, String> props = new HashMap<>();
      props.put("schema.registry.url", "http://localhost:8081");
      ProtoConfluentFormatConfig config =
          new ProtoConfluentFormatConfig("http://localhost:8081", TOPIC, false, props);
      config.onDeserializeError = onError;
      return new ProtoRowDataDeserializationSchema(row("i32", new TinyIntType()), null, config) {
        @Override
        SchemaRegistryClient createSchemaRegistryClient(InitializationContext context) {
          return client;
        }
      };
    }
  }
}
