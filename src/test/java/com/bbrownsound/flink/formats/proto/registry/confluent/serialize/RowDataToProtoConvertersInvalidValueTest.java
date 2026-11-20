package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoToRowDataConverters;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * Values the sink cannot write (issue #234): an enum name the enum does not define, a {@code NULL}
 * inside an array or map, and a {@code BIGINT} outside the range of a 32-bit field. Each fails with
 * an {@link IllegalArgumentException} naming the field and the value instead of a bare {@link
 * NullPointerException} or a silently wrapped number.
 */
class RowDataToProtoConvertersInvalidValueTest {

  private static final Descriptor SCHEMA =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package invalid;",
                      "import \"google/protobuf/wrappers.proto\";",
                      "enum Color {",
                      "  RED = 0;",
                      "  GREEN = 1;",
                      "}",
                      "message Values {",
                      "  Color color = 1;",
                      "  repeated Color colors = 2;",
                      "  map<string, Color> color_map = 3;",
                      "  repeated string names = 4;",
                      "  repeated int64 numbers = 5;",
                      "  map<string, string> labels = 6;",
                      "  map<string, Inner> inners = 7;",
                      "  uint32 u32 = 8;",
                      "  fixed32 f32 = 9;",
                      "  sfixed32 sf32 = 10;",
                      "  google.protobuf.UInt32Value u32_wrapper = 11;",
                      "  repeated uint32 u32_list = 12;",
                      "  message Inner {",
                      "    string name = 1;",
                      "  }",
                      "}"))
              .toDescriptor();

  // ---- helpers -------------------------------------------------------------------------------

  private static RowType row(String name, LogicalType type) {
    return new RowType(List.of(new RowField(name, type)));
  }

  private static Message write(RowType type, Object value) {
    return (Message)
        RowDataToProtoConverters.createConverter(type, SCHEMA).convert(GenericRowData.of(value));
  }

  private static RowData read(RowType type, Message message) throws Exception {
    final DynamicMessage parsed = DynamicMessage.parseFrom(SCHEMA, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(SCHEMA, type).convert(parsed);
  }

  private static Object protoField(Message message, String name) {
    return message.getField(SCHEMA.findFieldByName(name));
  }

  private static IllegalArgumentException writeFails(RowType type, Object value) {
    return assertThrows(IllegalArgumentException.class, () -> write(type, value));
  }

  private static void assertContains(String message, String... parts) {
    for (String part : parts) {
      assertTrue(message.contains(part), () -> "'" + message + "' should contain '" + part + "'");
    }
  }

  private static GenericMapData map(Object key, Object value) {
    final Map<Object, Object> map = new HashMap<>();
    map.put(key, value);
    return new GenericMapData(map);
  }

  // ---- enum names ----------------------------------------------------------------------------

  @Test
  void unknownEnumNameFailsNamingTheFieldTheValueAndTheValidNames() {
    final IllegalArgumentException e =
        writeFails(row("color", VarCharType.STRING_TYPE), StringData.fromString("BLUE"));
    assertContains(e.getMessage(), "'BLUE'", "invalid.Values.color", "invalid.Color", "RED, GREEN");
  }

  @Test
  void unknownEnumNameInAnArrayOrMapValueFails() {
    final IllegalArgumentException array =
        writeFails(
            row("colors", new ArrayType(VarCharType.STRING_TYPE)),
            new GenericArrayData(
                new Object[] {StringData.fromString("GREEN"), StringData.fromString("BLUE")}));
    assertContains(array.getMessage(), "'BLUE'", "invalid.Values.colors", "RED, GREEN");

    final IllegalArgumentException map =
        writeFails(
            row("color_map", new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE)),
            map(StringData.fromString("k"), StringData.fromString("BLUE")));
    assertContains(map.getMessage(), "'BLUE'", "invalid.Color", "RED, GREEN");
  }

  @Test
  void knownEnumNamesStillWrite() {
    final Message message =
        write(row("color", VarCharType.STRING_TYPE), StringData.fromString("GREEN"));
    assertEquals("GREEN", ((EnumValueDescriptor) protoField(message, "color")).getName());
  }

  // ---- NULL inside arrays and maps -----------------------------------------------------------

  @Test
  void nullArrayElementFailsNamingTheFieldAndTheIndex() {
    final IllegalArgumentException strings =
        writeFails(
            row("names", new ArrayType(VarCharType.STRING_TYPE)),
            new GenericArrayData(new Object[] {StringData.fromString("a"), null}));
    assertContains(strings.getMessage(), "invalid.Values.names", "index 1", "NULL");

    final IllegalArgumentException numbers =
        writeFails(
            row("numbers", new ArrayType(new BigIntType())),
            new GenericArrayData(new Object[] {null, 2L}));
    assertContains(numbers.getMessage(), "invalid.Values.numbers", "index 0", "NULL");
  }

  @Test
  void nullMapValueFailsNamingTheFieldAndTheKey() {
    final IllegalArgumentException strings =
        writeFails(
            row("labels", new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE)),
            map(StringData.fromString("team"), null));
    assertContains(strings.getMessage(), "invalid.Values.labels", "'team'", "NULL");

    final IllegalArgumentException messages =
        writeFails(
            row(
                "inners",
                new MapType(
                    VarCharType.STRING_TYPE,
                    new RowType(List.of(new RowField("name", VarCharType.STRING_TYPE))))),
            map(StringData.fromString("x"), null));
    assertContains(messages.getMessage(), "invalid.Values.inners", "'x'", "NULL");
  }

  @Test
  void nullMapKeyFailsNamingTheField() {
    final IllegalArgumentException e =
        writeFails(
            row("labels", new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE)),
            map(null, StringData.fromString("v")));
    assertContains(e.getMessage(), "invalid.Values.labels", "NULL key");
  }

  @Test
  void nullMultisetElementFailsNamingTheField() {
    final IllegalArgumentException e =
        writeFails(row("labels", new MultisetType(VarCharType.STRING_TYPE)), map(null, 1));
    assertContains(e.getMessage(), "invalid.Values.labels", "NULL key");
  }

  @Test
  void arraysAndMapsWithoutNullStillWrite() throws Exception {
    final RowType names = row("names", new ArrayType(VarCharType.STRING_TYPE));
    final Message array =
        write(
            names,
            new GenericArrayData(
                new Object[] {StringData.fromString("a"), StringData.fromString("b")}));
    assertEquals(List.of("a", "b"), protoField(array, "names"));

    final RowType labels =
        row("labels", new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE));
    final Message map = write(labels, map(StringData.fromString("k"), StringData.fromString("v")));
    assertEquals(
        StringData.fromString("v"),
        read(labels, map).getMap(0).valueArray().getString(0));
  }

  // ---- BIGINT into 32-bit fields -------------------------------------------------------------

  @Test
  void bigintIntoUnsigned32BitFieldsRoundTripsTheWholeRange() throws Exception {
    for (String name : new String[] {"u32", "f32", "u32_wrapper"}) {
      final RowType type = row(name, new BigIntType());
      for (long value : new long[] {0L, 5L, 2147483648L, 4294967295L}) {
        assertEquals(value, read(type, write(type, value)).getLong(0), name + " " + value);
      }
    }
    assertEquals(-1, protoField(write(row("u32", new BigIntType()), 4294967295L), "u32"));
  }

  @Test
  void bigintOutsideTheUnsigned32BitRangeFails() {
    for (String name : new String[] {"u32", "f32", "u32_wrapper"}) {
      for (long value : new long[] {4294967296L, 4294967301L, -1L, Long.MIN_VALUE}) {
        final IllegalArgumentException e = writeFails(row(name, new BigIntType()), value);
        assertContains(e.getMessage(), String.valueOf(value), "invalid.Values." + name, "4294967295");
      }
    }
  }

  @Test
  void bigintOutsideTheUnsigned32BitRangeFailsInAnArray() {
    final IllegalArgumentException e =
        writeFails(
            row("u32_list", new ArrayType(new BigIntType())),
            new GenericArrayData(new Object[] {1L, 4294967296L}));
    assertContains(e.getMessage(), "4294967296", "invalid.Values.u32_list");
  }

  @Test
  void bigintIntoSfixed32MustFitAnInt() throws Exception {
    final RowType type = row("sf32", new BigIntType());
    for (long value : new long[] {Integer.MIN_VALUE, -1L, 0L, Integer.MAX_VALUE}) {
      assertEquals((int) value, protoField(write(type, value), "sf32"));
    }
    for (long value : new long[] {Integer.MAX_VALUE + 1L, Integer.MIN_VALUE - 1L}) {
      final IllegalArgumentException e = writeFails(type, value);
      assertContains(e.getMessage(), String.valueOf(value), "invalid.Values.sf32", "2147483647");
    }
  }
}
