package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * A source column may be wider than the type derived for its field when it holds every value of
 * the field (GitHub issue #245): {@code BIGINT} for a signed 32-bit integer, {@code DOUBLE} for a
 * {@code float} and {@code DECIMAL} with enough integer digits for any integer. Narrower columns
 * are still rejected, by the converter and by the {@code message-class} check.
 */
class WideningReadsTest {

  private static final Descriptor WIDE =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package wide;",
                      "import \"google/protobuf/wrappers.proto\";",
                      "message Wide {",
                      "  int32 i32 = 1;",
                      "  sint32 s32 = 2;",
                      "  sfixed32 sf32 = 3;",
                      "  uint32 u32 = 4;",
                      "  fixed32 f32 = 5;",
                      "  int64 i64 = 6;",
                      "  sint64 s64 = 7;",
                      "  sfixed64 sf64 = 8;",
                      "  float f = 9;",
                      "  google.protobuf.Int32Value w_i32 = 10;",
                      "  google.protobuf.UInt32Value w_u32 = 11;",
                      "  google.protobuf.Int64Value w_i64 = 12;",
                      "  google.protobuf.FloatValue w_f = 13;",
                      "  repeated int32 list = 14;",
                      "  map<string, int32> counts = 15;",
                      "  repeated float ratios = 16;",
                      "  double d = 17;",
                      "  string s = 18;",
                      "  enum Color { RED = 0; GREEN = 1; }",
                      "  Color color = 19;",
                      "  google.protobuf.StringValue w_s = 20;",
                      "  google.protobuf.UInt64Value w_u64 = 21;",
                      "}"))
              .toDescriptor();

  private static final List<String> SIGNED_32 = List.of("i32", "s32", "sf32", "w_i32");
  private static final List<String> UNSIGNED_32 = List.of("u32", "f32", "w_u32");
  private static final List<String> SIGNED_64 = List.of("i64", "s64", "sf64", "w_i64");
  private static final List<String> FLOATS = List.of("f", "w_f");

  // ---- converter: accepted widenings ---------------------------------------------------------

  @Test
  void bigintReadsSigned32BitFields() throws Exception {
    for (String name : SIGNED_32) {
      for (int value : new int[] {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE}) {
        RowData row = read(row(name, new BigIntType()), wide(name, value));
        assertEquals((long) value, row.getLong(0), name + " = " + value);
      }
    }
  }

  @Test
  void doubleReadsFloatFields() throws Exception {
    for (String name : FLOATS) {
      for (float value :
          new float[] {
            0.1f, -1.5f, Float.MIN_VALUE, Float.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.NaN
          }) {
        RowData row = read(row(name, new DoubleType()), wide(name, value));
        assertEquals((double) value, row.getDouble(0), name + " = " + value);
      }
    }
  }

  @Test
  void decimalReadsSigned32BitFields() throws Exception {
    for (String name : SIGNED_32) {
      for (int value : new int[] {Integer.MIN_VALUE, -1, 0, Integer.MAX_VALUE}) {
        assertDecimal(name, new DecimalType(10, 0), value, BigDecimal.valueOf(value));
        assertDecimal(name, new DecimalType(12, 2), value, BigDecimal.valueOf(value));
      }
    }
  }

  @Test
  void decimalReadsUnsigned32BitFieldsAsUnsigned() throws Exception {
    for (String name : UNSIGNED_32) {
      assertDecimal(name, new DecimalType(10, 0), 0, BigDecimal.ZERO);
      assertDecimal(
          name, new DecimalType(10, 0), Integer.MAX_VALUE, BigDecimal.valueOf(2147483647L));
      assertDecimal(name, new DecimalType(10, 0), -1, new BigDecimal("4294967295"));
      assertDecimal(name, new DecimalType(20, 3), -1, new BigDecimal("4294967295"));
    }
  }

  @Test
  void decimalReadsSigned64BitFields() throws Exception {
    for (String name : SIGNED_64) {
      for (long value : new long[] {Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE}) {
        assertDecimal(name, new DecimalType(19, 0), value, BigDecimal.valueOf(value));
        assertDecimal(name, new DecimalType(20, 1), value, BigDecimal.valueOf(value));
      }
    }
  }

  @Test
  void arrayAndMapValuesWiden() throws Exception {
    Message message =
        DynamicMessage.newBuilder(WIDE)
            .addRepeatedField(WIDE.findFieldByName("list"), Integer.MIN_VALUE)
            .addRepeatedField(WIDE.findFieldByName("ratios"), 0.1f)
            .build();
    RowData row =
        read(
            new RowType(
                List.of(
                    new RowField("list", new ArrayType(new BigIntType())),
                    new RowField("ratios", new ArrayType(new DoubleType())))),
            message);
    assertEquals((long) Integer.MIN_VALUE, row.getArray(0).getLong(0));
    assertEquals((double) 0.1f, row.getArray(1).getDouble(0));

    FieldDescriptor counts = WIDE.findFieldByName("counts");
    Message entry =
        DynamicMessage.newBuilder(counts.getMessageType())
            .setField(counts.getMessageType().findFieldByName("key"), "a")
            .setField(counts.getMessageType().findFieldByName("value"), -7)
            .build();
    RowData mapRow =
        read(
            row("counts", new MapType(new VarCharType(VarCharType.MAX_LENGTH), new BigIntType())),
            DynamicMessage.newBuilder(WIDE).addRepeatedField(counts, entry).build());
    assertEquals(-7L, mapRow.getMap(0).valueArray().getLong(0));
  }

  // ---- converter: narrowing is still rejected ------------------------------------------------

  @Test
  void decimalTooNarrowForTheFieldIsRejected() {
    for (String name : SIGNED_32) {
      assertTooNarrow(name, new DecimalType(9, 0), 10);
      assertTooNarrow(name, new DecimalType(10, 1), 10);
    }
    for (String name : UNSIGNED_32) {
      assertTooNarrow(name, new DecimalType(9, 0), 10);
    }
    for (String name : SIGNED_64) {
      assertTooNarrow(name, new DecimalType(18, 0), 19);
      assertTooNarrow(name, new DecimalType(20, 2), 19);
    }
  }

  @Test
  void otherPairsAreStillRejected() {
    assertRejected("i64", new IntType());
    assertRejected("d", new FloatType());
    assertRejected("i32", new DoubleType());
    assertRejected("u32", new IntType());
    assertRejected("color", new BigIntType());
    assertRejected("s", new BigIntType());
    assertRejected("s", new DecimalType(20, 0));
    assertRejected("d", new DecimalType(38, 0));
  }

  // ---- message-class check -------------------------------------------------------------------

  @Test
  void messageClassCheckAcceptsWidenings() {
    for (String name : SIGNED_32) {
      assertAccepted(name, new BigIntType());
      assertAccepted(name, new DecimalType(10, 0));
    }
    for (String name : UNSIGNED_32) {
      assertAccepted(name, new DecimalType(10, 0));
    }
    for (String name : SIGNED_64) {
      assertAccepted(name, new DecimalType(19, 0));
    }
    for (String name : FLOATS) {
      assertAccepted(name, new DoubleType());
    }
    assertAccepted("list", new ArrayType(new BigIntType()));
    assertAccepted("ratios", new ArrayType(new DoubleType()));
    assertAccepted(
        "counts", new MapType(new VarCharType(VarCharType.MAX_LENGTH), new DecimalType(10, 0)));
    // The uint64 alternative is unchanged: any precision, a value that does not fit reads as NULL.
    assertAccepted("w_u64", new DecimalType(5, 0));
  }

  @Test
  void messageClassCheckRejectsNarrowing() {
    String message = validationError("i32", new DecimalType(9, 0));
    assertTrue(message.contains("column 'i32' is DECIMAL(9, 0)"), message);
    assertTrue(message.contains("(int32) reads as INT"), message);
    assertTrue(message.contains("at least 10 digits before the decimal point"), message);

    message = validationError("i64", new DecimalType(18, 0));
    assertTrue(message.contains("(int64) reads as BIGINT"), message);
    assertTrue(message.contains("at least 19 digits before the decimal point"), message);

    message = validationError("i64", new IntType());
    assertTrue(message.contains("(int64) reads as BIGINT"), message);
    assertFalse(message.contains("decimal point"), message);

    message = validationError("i32", new DoubleType());
    assertTrue(message.contains("(int32) reads as INT"), message);

    message = validationError("d", new FloatType());
    assertTrue(message.contains("(double) reads as DOUBLE"), message);

    message = validationError("color", new BigIntType());
    assertTrue(message.contains("(enum) reads as STRING"), message);

    message = validationError("w_s", new DoubleType());
    assertTrue(message.contains("reads as STRING"), message);

    message = validationError("s", new DecimalType(20, 0));
    assertTrue(message.contains("(string) reads as STRING"), message);
    assertFalse(message.contains("decimal point"), message);
  }

  // ---- full deserialization schema: a field widened between schema versions -----------------

  @Test
  void tableDeclaredForTheWiderSchemaReadsRecordsOfTheNarrowerOne() throws Exception {
    MockSchemaRegistryClient client =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    Descriptor v1 = order("int32 amount = 1; float ratio = 2;");
    Descriptor v2 = order("int64 amount = 1; double ratio = 2;");
    byte[] old = serialize(client, "orders-v1", message(v1, -5, 0.25f));
    byte[] current = serialize(client, "orders-v2", message(v2, 1L << 40, 0.1d));

    RowType table =
        new RowType(
            List.of(
                new RowField("amount", new BigIntType()), new RowField("ratio", new DoubleType())));
    Map<String, String> props = new HashMap<>();
    props.put("schema.registry.url", "http://localhost:8081");
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://localhost:8081", "orders", false, props);
    ProtoRowDataDeserializationSchema schema =
        new ProtoRowDataDeserializationSchema(table, null, config) {
          @Override
          SchemaRegistryClient createSchemaRegistryClient(InitializationContext context) {
            return client;
          }
        };
    schema.open(null);

    RowData oldRow = schema.deserialize(old);
    assertEquals(-5L, oldRow.getLong(0));
    assertEquals(0.25d, oldRow.getDouble(1));
    RowData currentRow = schema.deserialize(current);
    assertEquals(1L << 40, currentRow.getLong(0));
    assertEquals(0.1d, currentRow.getDouble(1));
  }

  // ---- helpers -------------------------------------------------------------------------------

  private static Descriptor order(String fields) throws Exception {
    return (Descriptor)
        new ProtobufSchema("syntax = \"proto3\"; package shop; message Order { " + fields + " }")
            .toDescriptor();
  }

  private static Message message(Descriptor descriptor, Object amount, Object ratio) {
    return DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("amount"), amount)
        .setField(descriptor.findFieldByName("ratio"), ratio)
        .build();
  }

  private static byte[] serialize(SchemaRegistryClient client, String topic, Message message) {
    try (KafkaProtobufSerializer<Message> serializer = new KafkaProtobufSerializer<>(client)) {
      serializer.configure(
          Map.of("schema.registry.url", "mock://unused", "auto.register.schemas", "true"), false);
      return serializer.serialize(topic, message);
    }
  }

  private static void assertDecimal(String name, DecimalType type, Object value, BigDecimal want)
      throws Exception {
    RowData row = read(row(name, type), wide(name, value));
    DecimalData got = row.getDecimal(0, type.getPrecision(), type.getScale());
    assertEquals(0, want.compareTo(got.toBigDecimal()), name + " " + type + " = " + value);
  }

  private static void assertTooNarrow(String name, DecimalType type, int digits) {
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> ProtoToRowDataConverters.createConverter(WIDE, row(name, type)),
            name + " " + type);
    String message = e.getMessage();
    assertTrue(message.contains("'wide.Wide." + name + "'"), message);
    assertTrue(message.contains(type.asSummaryString()), message);
    assertTrue(message.contains("at least " + digits + " digits"), message);
  }

  private static void assertRejected(String name, LogicalType type) {
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> ProtoToRowDataConverters.createConverter(WIDE, row(name, type)),
            name + " " + type);
    assertTrue(e.getMessage().contains("Unsupported protobuf type"), e.getMessage());
  }

  private static void assertAccepted(String name, LogicalType type) {
    assertDoesNotThrow(
        () -> SourceMessageValidator.validateRowType(WIDE, row(name, type), "wide.Wide"),
        name + " " + type);
  }

  private static String validationError(String name, LogicalType type) {
    return assertThrows(
            ValidationException.class,
            () -> SourceMessageValidator.validateRowType(WIDE, row(name, type), "wide.Wide"),
            name + " " + type)
        .getMessage();
  }

  private static RowType row(String name, LogicalType type) {
    return new RowType(List.of(new RowField(name, type)));
  }

  private static Message wide(String name, Object value) {
    FieldDescriptor field = WIDE.findFieldByName(name);
    Object protoValue =
        field.getType() == FieldDescriptor.Type.MESSAGE
            ? DynamicMessage.newBuilder(field.getMessageType())
                .setField(field.getMessageType().findFieldByName("value"), value)
                .build()
            : value;
    return DynamicMessage.newBuilder(WIDE).setField(field, protoValue).build();
  }

  private static RowData read(RowType type, Message message) throws Exception {
    DynamicMessage parsed = DynamicMessage.parseFrom(WIDE, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(WIDE, type).convert(parsed);
  }
}
