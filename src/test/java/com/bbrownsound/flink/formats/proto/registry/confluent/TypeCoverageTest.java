package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoToRowDataConverters;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.SourceMessageValidator;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.RowDataToProtoConverters;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RowTypeToProto;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt64Value;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * Column types a table can declare for a Protobuf field in place of the derived one (issue #174):
 * each is read from and written back to the same Protobuf value, accepted by the {@code
 * message-class} check, and (where it has one) derived to the Protobuf type it came from.
 */
class TypeCoverageTest {

  private static final String CLASS_NAME = "com.example.Coverage";

  private static final Descriptor COVERAGE =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package coverage;",
                      "import \"google/protobuf/timestamp.proto\";",
                      "import \"google/protobuf/wrappers.proto\";",
                      "import \"google/protobuf/struct.proto\";",
                      "import \"google/protobuf/any.proto\";",
                      "import \"google/protobuf/duration.proto\";",
                      "enum Color {",
                      "  RED = 0;",
                      "  GREEN = 1;",
                      "}",
                      "message Coverage {",
                      "  google.protobuf.Timestamp ts = 1;",
                      "  repeated google.protobuf.Timestamp ts_list = 2;",
                      "  uint64 u64 = 3;",
                      "  fixed64 f64 = 4;",
                      "  google.protobuf.UInt64Value u64_wrapper = 5;",
                      "  uint32 u32 = 6;",
                      "  google.protobuf.UInt32Value u32_wrapper = 7;",
                      "  google.protobuf.Struct struct = 8;",
                      "  google.protobuf.Value value = 9;",
                      "  google.protobuf.ListValue list = 10;",
                      "  repeated google.protobuf.Struct structs = 11;",
                      "  google.protobuf.Any any = 12;",
                      "  Inner inner = 13;",
                      "  Color color = 14;",
                      "  repeated Color colors = 15;",
                      "  map<string, Color> color_map = 16;",
                      "  google.protobuf.Duration duration = 17;",
                      "  message Inner {",
                      "    string inner_name = 1;",
                      "    int32 count = 2;",
                      "  }",
                      "}"))
              .toDescriptor();

  // ---- helpers -------------------------------------------------------------------------------

  private static RowType row(RowField... fields) {
    return new RowType(List.of(fields));
  }

  private static RowField field(String name, LogicalType type) {
    return new RowField(name, type);
  }

  private static Message write(RowType type, RowData row) {
    return (Message) RowDataToProtoConverters.createConverter(type, COVERAGE).convert(row);
  }

  private static RowData read(RowType type, Message message) throws Exception {
    final DynamicMessage parsed = DynamicMessage.parseFrom(COVERAGE, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(COVERAGE, type).convert(parsed);
  }

  private static Object protoField(Message message, String name) {
    return message.getField(message.getDescriptorForType().findFieldByName(name));
  }

  private static FieldDescriptor derivedField(RowType type, String name) {
    return RowTypeToProto.fromRowType(type, "Row", "derived").findFieldByName(name);
  }

  // ---- google.protobuf.Timestamp as TIMESTAMP(p) --------------------------------------------

  @Test
  void timestampWithoutTimeZoneRoundTripsAsUtc() throws Exception {
    final RowType type = row(field("ts", new TimestampType(9)));
    final LocalDateTime wallClock = LocalDateTime.of(2024, 3, 10, 2, 30, 15, 123_456_789);
    final GenericRowData in = GenericRowData.of(TimestampData.fromLocalDateTime(wallClock));

    final Message message = write(type, in);
    final Message ts = (Message) protoField(message, "ts");
    // 2024-03-10T02:30:15.123456789 read as UTC.
    assertEquals(1710037815L, protoField(ts, "seconds"));
    assertEquals(123_456_789, protoField(ts, "nanos"));

    final RowData out = read(type, message);
    assertEquals(wallClock, out.getTimestamp(0, 9).toLocalDateTime());
  }

  @Test
  void timestampWithoutTimeZoneReadsBeforeTheEpochAndUnsetAsNull() throws Exception {
    final RowType type = row(field("ts", new TimestampType(3)));
    final Message message =
        DynamicMessage.newBuilder(COVERAGE)
            .setField(
                COVERAGE.findFieldByName("ts"),
                Timestamp.newBuilder().setSeconds(-1).setNanos(500_000_000).build())
            .build();
    assertEquals(
        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000),
        read(type, message).getTimestamp(0, 3).toLocalDateTime());

    assertNull(read(type, DynamicMessage.getDefaultInstance(COVERAGE)).getTimestamp(0, 3));
  }

  @Test
  void timestampWithoutTimeZoneInAnArrayRoundTrips() throws Exception {
    final RowType type = row(field("ts_list", new ArrayType(new TimestampType(6))));
    final LocalDateTime wallClock = LocalDateTime.of(2000, 1, 1, 0, 0, 0, 1_000);
    final GenericRowData in =
        GenericRowData.of(
            new GenericArrayData(new Object[] {TimestampData.fromLocalDateTime(wallClock)}));
    final RowData out = read(type, write(type, in));
    assertEquals(wallClock, out.getArray(0).getTimestamp(0, 6).toLocalDateTime());
  }

  @Test
  void timestampWithoutTimeZonePassesTheMessageClassCheck() {
    SourceMessageValidator.validateRowType(
        COVERAGE, row(field("ts", new TimestampType(3))), CLASS_NAME);
  }

  @Test
  void timestampWithoutTimeZoneDerivesAGoogleProtobufTimestamp() {
    final FieldDescriptor ts = derivedField(row(field("ts", new TimestampType(3))), "ts");
    assertEquals("google.protobuf.Timestamp", ts.getMessageType().getFullName());
  }

  @Test
  void theCheckStillRejectsATimestampForAScalarField() {
    final Descriptor scalar =
        (Descriptor)
            new ProtobufSchema("syntax = \"proto3\"; message S { int64 ts = 1; }").toDescriptor();
    assertThrows(
        ValidationException.class,
        () ->
            SourceMessageValidator.validateRowType(
                scalar, row(field("ts", new TimestampType(3))), CLASS_NAME));
  }

  // ---- uint64 / fixed64 as DECIMAL(20, 0) ---------------------------------------------------

  private static final BigDecimal UINT64_MAX = new BigDecimal("18446744073709551615");

  private static DecimalData decimal(BigDecimal value) {
    return DecimalData.fromBigDecimal(value, 20, 0);
  }

  @Test
  void unsigned64BitFieldsRoundTripAsDecimal() throws Exception {
    final DecimalType decimal = new DecimalType(20, 0);
    final RowType type =
        row(field("u64", decimal), field("f64", decimal), field("u64_wrapper", decimal));
    final BigDecimal aboveLongMax = new BigDecimal("9223372036854775808");
    final GenericRowData in =
        GenericRowData.of(decimal(UINT64_MAX), decimal(aboveLongMax), decimal(UINT64_MAX));

    final Message message = write(type, in);
    // Java holds the unsigned 64 bits in a signed long.
    assertEquals(-1L, protoField(message, "u64"));
    assertEquals(Long.MIN_VALUE, protoField(message, "f64"));
    assertEquals(
        UInt64Value.of(-1L),
        UInt64Value.parseFrom(((Message) protoField(message, "u64_wrapper")).toByteString()));

    final RowData out = read(type, message);
    assertEquals(UINT64_MAX, out.getDecimal(0, 20, 0).toBigDecimal());
    assertEquals(aboveLongMax, out.getDecimal(1, 20, 0).toBigDecimal());
    assertEquals(UINT64_MAX, out.getDecimal(2, 20, 0).toBigDecimal());
  }

  @Test
  void anUnsignedValueTooWideForTheDeclaredPrecisionReadsAsNull() throws Exception {
    final RowType type = row(field("u64", new DecimalType(5, 0)));
    final Message message =
        DynamicMessage.newBuilder(COVERAGE).setField(COVERAGE.findFieldByName("u64"), -1L).build();
    assertNull(read(type, message).getDecimal(0, 5, 0));
  }

  @Test
  void writingADecimalOutsideTheUnsignedRangeFails() {
    final RowType type = row(field("u64", new DecimalType(21, 1)));
    for (String value : new String[] {"-1", "18446744073709551616", "1.5"}) {
      final GenericRowData in =
          GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal(value), 21, 1));
      final IllegalArgumentException e =
          assertThrows(IllegalArgumentException.class, () -> write(type, in));
      assertTrue(e.getMessage().contains("coverage.Coverage.u64"), e.getMessage());
    }
  }

  @Test
  void bigintKeepsTheSame64BitsForUint64() throws Exception {
    final RowType type =
        row(field("u64", new BigIntType()), field("u64_wrapper", new BigIntType()));
    final GenericRowData in = GenericRowData.of(-1L, -1L);
    final Message message = write(type, in);
    final RowData out = read(type, message);
    // 2^64 - 1 reads as -1: BIGINT has no room for it. Writing -1 back writes 2^64 - 1.
    assertEquals(-1L, out.getLong(0));
    assertEquals(-1L, out.getLong(1));
  }

  @Test
  void uint32ReadsAsItsUnsignedValueInABigint() throws Exception {
    final RowType type =
        row(field("u32", new BigIntType()), field("u32_wrapper", new BigIntType()));
    final GenericRowData in = GenericRowData.of(4294967295L, 4294967295L);
    final Message message = write(type, in);
    assertEquals(-1, protoField(message, "u32"));
    final RowData out = read(type, message);
    assertEquals(4294967295L, out.getLong(0));
    assertEquals(4294967295L, out.getLong(1));
  }

  @Test
  void decimalPassesTheMessageClassCheckForUnsigned64BitFieldsOnly() {
    final DecimalType decimal = new DecimalType(20, 0);
    SourceMessageValidator.validateRowType(
        COVERAGE,
        row(field("u64", decimal), field("f64", decimal), field("u64_wrapper", decimal)),
        CLASS_NAME);
    assertThrows(
        ValidationException.class,
        () ->
            SourceMessageValidator.validateRowType(
                COVERAGE, row(field("u32", decimal)), CLASS_NAME));
    assertThrows(
        ValidationException.class,
        () ->
            SourceMessageValidator.validateRowType(
                COVERAGE, row(field("u32_wrapper", decimal)), CLASS_NAME));
  }

  // ---- google.protobuf.Struct / Value / ListValue as JSON STRING -----------------------------

  private static StringData str(String value) {
    return StringData.fromString(value);
  }

  @Test
  void structValueAndListValueRoundTripAsJson() throws Exception {
    final RowType type =
        row(
            field("struct", VarCharType.STRING_TYPE),
            field("value", VarCharType.STRING_TYPE),
            field("list", VarCharType.STRING_TYPE),
            field("structs", new ArrayType(VarCharType.STRING_TYPE)));
    final String struct = "{\"a\":1.5,\"b\":[true,null,\"x\"],\"c\":{\"d\":\"e\"}}";
    final GenericRowData in =
        GenericRowData.of(
            str(struct),
            str("\"text\""),
            str("[1.0,{\"k\":false}]"),
            new GenericArrayData(new Object[] {str("{}"), str("{\"n\":2.0}")}));

    final Message message = write(type, in);
    final Struct written =
        Struct.parseFrom(((Message) protoField(message, "struct")).toByteString());
    assertEquals(
        "e", written.getFieldsOrThrow("c").getStructValue().getFieldsOrThrow("d").getStringValue());

    final RowData out = read(type, message);
    assertEquals(struct, out.getString(0).toString());
    assertEquals("\"text\"", out.getString(1).toString());
    assertEquals("[1.0,{\"k\":false}]", out.getString(2).toString());
    assertEquals("{}", out.getArray(3).getString(0).toString());
    assertEquals("{\"n\":2.0}", out.getArray(3).getString(1).toString());
  }

  @Test
  void anyMessageDeclaredAsStringRoundTripsAsJsonWithProtobufFieldNames() throws Exception {
    final RowType type = row(field("inner", VarCharType.STRING_TYPE));
    final GenericRowData in = GenericRowData.of(str("{\"inner_name\":\"a\",\"count\":3}"));
    final RowData out = read(type, write(type, in));
    assertEquals("{\"inner_name\":\"a\",\"count\":3}", out.getString(0).toString());
  }

  @Test
  void writingInvalidJsonFails() {
    final RowType type = row(field("struct", VarCharType.STRING_TYPE));
    final IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> write(type, GenericRowData.of(str("[1, 2]"))));
    assertTrue(e.getMessage().contains("google.protobuf.Struct"), e.getMessage());
  }

  @Test
  void derivedDdlMapsStructValueAndListValueToString() {
    final RowType derived = (RowType) ProtoToLogicalType.toLogicalType(COVERAGE);
    for (String name : new String[] {"struct", "value", "list"}) {
      final LogicalType column = derived.getTypeAt(derived.getFieldIndex(name));
      assertEquals(LogicalTypeRoot.VARCHAR, column.getTypeRoot(), name);
      assertTrue(column.isNullable(), name);
    }
    final ArrayType structs = (ArrayType) derived.getTypeAt(derived.getFieldIndex("structs"));
    assertEquals(LogicalTypeRoot.VARCHAR, structs.getElementType().getTypeRoot());
    // The derived columns read the topic, so they pass the message-class check.
    SourceMessageValidator.validateRowType(COVERAGE, derived, CLASS_NAME);
  }

  @Test
  void stringPassesTheMessageClassCheckForAMessageButNotForAny() {
    SourceMessageValidator.validateRowType(
        COVERAGE, row(field("inner", VarCharType.STRING_TYPE)), CLASS_NAME);
    assertThrows(
        ValidationException.class,
        () ->
            SourceMessageValidator.validateRowType(
                COVERAGE, row(field("any", VarCharType.STRING_TYPE)), CLASS_NAME));
  }

  // ---- google.protobuf.Any -------------------------------------------------------------------

  @Test
  void anyIsDerivedAndRoundTripsAsARowOfTypeUrlAndValue() throws Exception {
    final RowType derived = (RowType) ProtoToLogicalType.toLogicalType(COVERAGE);
    final RowType any = (RowType) derived.getTypeAt(derived.getFieldIndex("any"));
    assertEquals(List.of("type_url", "value"), any.getFieldNames());
    assertEquals(LogicalTypeRoot.VARCHAR, any.getTypeAt(0).getTypeRoot());
    assertEquals(LogicalTypeRoot.VARBINARY, any.getTypeAt(1).getTypeRoot());

    final RowType type = row(field("any", any));
    final byte[] packed = Timestamp.newBuilder().setSeconds(7).build().toByteArray();
    final GenericRowData in =
        GenericRowData.of(
            GenericRowData.of(str("type.googleapis.com/google.protobuf.Timestamp"), packed));
    final Message message = write(type, in);
    final Any written = Any.parseFrom(((Message) protoField(message, "any")).toByteString());
    assertEquals(7L, written.unpack(Timestamp.class).getSeconds());

    final RowData out = read(type, message).getRow(0, 2);
    assertEquals("type.googleapis.com/google.protobuf.Timestamp", out.getString(0).toString());
    assertArrayEquals(packed, out.getBinary(1));
  }

  // ---- enum as INT ---------------------------------------------------------------------------

  @Test
  void enumRoundTripsAsItsNumber() throws Exception {
    final RowType type =
        row(
            field("color", new IntType()),
            field("colors", new ArrayType(new IntType())),
            field("color_map", new MapType(VarCharType.STRING_TYPE, new IntType())));
    final GenericRowData in =
        GenericRowData.of(
            1,
            new GenericArrayData(new Object[] {0, 1, 7}),
            new GenericMapData(Map.of(str("k"), 1)));

    final Message message = write(type, in);
    assertEquals("GREEN", ((EnumValueDescriptor) protoField(message, "color")).getName());

    final RowData out = read(type, message);
    assertEquals(1, out.getInt(0));
    assertArrayEquals(new int[] {0, 1, 7}, out.getArray(1).toIntArray());
    assertEquals(1, out.getMap(2).valueArray().getInt(0));
  }

  @Test
  void anUnknownEnumNumberRoundTripsInAnOpenEnum() throws Exception {
    final RowType type = row(field("color", new IntType()));
    final Message message = write(type, GenericRowData.of(42));
    assertEquals(42, ((EnumValueDescriptor) protoField(message, "color")).getNumber());
    assertEquals(42, read(type, message).getInt(0));
    // Reading it as STRING still gives a name, as it always has.
    assertTrue(
        read(row(field("color", VarCharType.STRING_TYPE)), message)
            .getString(0)
            .toString()
            .contains("42"));
  }

  @Test
  void writingAnUnknownNumberToAClosedEnumFails() {
    final Descriptor closed =
        (Descriptor)
            new ProtobufSchema(
                    "syntax = \"proto2\"; package closed; enum Shade { LIGHT = 1; DARK = 2; }"
                        + " message C { optional Shade shade = 1; }")
                .toDescriptor();
    final RowType type = row(field("shade", new IntType()));
    final RowDataToProtoConverters.RowDataToProtoConverter converter =
        RowDataToProtoConverters.createConverter(type, closed);
    final Message known = (Message) converter.convert(GenericRowData.of(2));
    assertEquals(
        "DARK", ((EnumValueDescriptor) known.getField(closed.findFieldByName("shade"))).getName());
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> converter.convert(GenericRowData.of(3)));
    assertTrue(e.getMessage().contains("closed.Shade"), e.getMessage());
  }

  @Test
  void intPassesTheMessageClassCheckForAnEnum() {
    SourceMessageValidator.validateRowType(
        COVERAGE,
        row(field("color", new IntType()), field("colors", new ArrayType(new IntType()))),
        CLASS_NAME);
  }

  // ---- recursive messages ---------------------------------------------------------------------

  private static final Descriptor TREE =
      (Descriptor)
          new ProtobufSchema(
                  "syntax = \"proto3\"; package rec; message Tree { string name = 1; Tree child ="
                      + " 2; repeated Tree children = 3; }")
              .toDescriptor();

  private static Message tree(String name, Message child) {
    final DynamicMessage.Builder builder =
        DynamicMessage.newBuilder(TREE).setField(TREE.findFieldByName("name"), name);
    if (child != null) {
      builder.setField(TREE.findFieldByName("child"), child);
    }
    return builder.build();
  }

  private static Message writeTree(RowType type, RowData row) {
    return (Message) RowDataToProtoConverters.createConverter(type, TREE).convert(row);
  }

  private static RowData readTree(RowType type, Message message) throws Exception {
    final DynamicMessage parsed = DynamicMessage.parseFrom(TREE, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(TREE, type).convert(parsed);
  }

  @Test
  void aRecursiveMessageReadsIntoARowNestedToAFixedDepth() throws Exception {
    final RowType leaf = row(field("name", VarCharType.STRING_TYPE));
    final RowType type =
        row(
            field("name", VarCharType.STRING_TYPE),
            field("child", row(field("name", VarCharType.STRING_TYPE), field("child", leaf))));
    final Message deep = tree("a", tree("b", tree("c", tree("d", null))));

    final RowData out = readTree(type, deep);
    assertEquals("a", out.getString(0).toString());
    assertEquals("b", out.getRow(1, 2).getString(0).toString());
    // Levels below the declared depth are left out.
    assertEquals("c", out.getRow(1, 2).getRow(1, 1).getString(0).toString());

    final Message written = writeTree(type, out);
    assertEquals(
        tree("a", tree("b", tree("c", null))),
        DynamicMessage.parseFrom(TREE, written.toByteArray()));
    SourceMessageValidator.validateRowType(TREE, type, CLASS_NAME);
  }

  @Test
  void aRecursiveFieldRoundTripsAsBytes() throws Exception {
    final RowType type =
        row(
            field("name", VarCharType.STRING_TYPE),
            field("child", new VarBinaryType(VarBinaryType.MAX_LENGTH)),
            field("children", new ArrayType(new VarBinaryType(VarBinaryType.MAX_LENGTH))));
    final Message child = tree("b", tree("c", null));
    final Message message =
        DynamicMessage.newBuilder((DynamicMessage) tree("a", child))
            .addRepeatedField(TREE.findFieldByName("children"), tree("x", null))
            .build();

    final RowData out = readTree(type, message);
    assertArrayEquals(child.toByteArray(), out.getBinary(1));
    assertArrayEquals(tree("x", null).toByteArray(), out.getArray(2).getBinary(0));

    final Message written = writeTree(type, out);
    assertEquals(message, DynamicMessage.parseFrom(TREE, written.toByteArray()));
    SourceMessageValidator.validateRowType(TREE, type, CLASS_NAME);
  }

  @Test
  void aRecursiveFieldRoundTripsAsJson() throws Exception {
    final RowType type =
        row(field("name", VarCharType.STRING_TYPE), field("child", VarCharType.STRING_TYPE));
    final Message message = tree("a", tree("b", tree("c", null)));

    final RowData out = readTree(type, message);
    assertEquals("{\"name\":\"b\",\"child\":{\"name\":\"c\"}}", out.getString(1).toString());
    assertEquals(message, DynamicMessage.parseFrom(TREE, writeTree(type, out).toByteArray()));
    SourceMessageValidator.validateRowType(TREE, type, CLASS_NAME);
  }

  @Test
  void writingBytesThatAreNotTheMessageFails() {
    final RowType type = row(field("child", new VarBinaryType(VarBinaryType.MAX_LENGTH)));
    final IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> writeTree(type, GenericRowData.of((Object) new byte[] {(byte) 0xff})));
    assertTrue(e.getMessage().contains("rec.Tree"), e.getMessage());
  }

  @Test
  void anyRoundTripsAsBytes() throws Exception {
    final RowType type = row(field("any", new VarBinaryType(VarBinaryType.MAX_LENGTH)));
    final Any any = Any.pack(Timestamp.newBuilder().setSeconds(9).build());
    final RowData out = read(type, write(type, GenericRowData.of((Object) any.toByteArray())));
    assertEquals(any, Any.parseFrom(out.getBinary(0)));
    SourceMessageValidator.validateRowType(COVERAGE, type, CLASS_NAME);
  }

  @Test
  void bytesForAWrapperStillReadsItsValueField() throws Exception {
    final Descriptor wrapped =
        (Descriptor)
            new ProtobufSchema(
                    "syntax = \"proto3\"; import \"google/protobuf/wrappers.proto\";"
                        + " message W { google.protobuf.BytesValue b = 1; }")
                .toDescriptor();
    final RowType type = row(field("b", new VarBinaryType(VarBinaryType.MAX_LENGTH)));
    final Message message =
        (Message)
            RowDataToProtoConverters.createConverter(type, wrapped)
                .convert(GenericRowData.of((Object) new byte[] {1, 2}));
    final RowData out =
        (RowData) ProtoToRowDataConverters.createConverter(wrapped, type).convert(message);
    assertArrayEquals(new byte[] {1, 2}, out.getBinary(0));
  }

  @Test
  void aRecursiveFieldDeclaredAsAnotherScalarFailsTheCheck() {
    assertThrows(
        ValidationException.class,
        () ->
            SourceMessageValidator.validateRowType(
                TREE, row(field("child", new IntType())), CLASS_NAME));
  }

  // ---- google.protobuf.Duration as INTERVAL DAY TO SECOND ------------------------------------

  private static DayTimeIntervalType dayToSecond() {
    return new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 3);
  }

  @Test
  void durationRoundTripsAsADayToSecondInterval() throws Exception {
    final RowType type = row(field("duration", dayToSecond()));
    // 1 day, 2 hours, 3.5 seconds, and a negative duration.
    for (long millis : new long[] {93_603_500L, -1_500L, 0L}) {
      final Message message = write(type, GenericRowData.of(millis));
      final com.google.protobuf.Duration duration =
          com.google.protobuf.Duration.parseFrom(
              ((Message) protoField(message, "duration")).toByteString());
      assertEquals(millis / 1000, duration.getSeconds());
      assertEquals((millis % 1000) * 1_000_000, duration.getNanos());
      assertEquals(millis, read(type, message).getLong(0));
    }
  }

  @Test
  void durationNanosBelowAMillisecondAreDropped() throws Exception {
    final RowType type = row(field("duration", dayToSecond()));
    final Message message =
        DynamicMessage.newBuilder(COVERAGE)
            .setField(
                COVERAGE.findFieldByName("duration"),
                com.google.protobuf.Duration.newBuilder().setSeconds(-2).setNanos(-999_999).build())
            .build();
    assertEquals(-2_000L, read(type, message).getLong(0));
    assertTrue(read(type, DynamicMessage.getDefaultInstance(COVERAGE)).isNullAt(0));
  }

  @Test
  void durationIsStillDerivedAsARowOfSecondsAndNanos() throws Exception {
    final RowType derived = (RowType) ProtoToLogicalType.toLogicalType(COVERAGE);
    final RowType duration = (RowType) derived.getTypeAt(derived.getFieldIndex("duration"));
    assertEquals(List.of("seconds", "nanos"), duration.getFieldNames());
  }

  @Test
  void intervalPassesTheMessageClassCheckAndDerivesADuration() {
    final RowType type = row(field("duration", dayToSecond()));
    SourceMessageValidator.validateRowType(COVERAGE, type, CLASS_NAME);
    assertEquals(
        "google.protobuf.Duration", derivedField(type, "duration").getMessageType().getFullName());
    assertThrows(
        ValidationException.class,
        () ->
            SourceMessageValidator.validateRowType(
                COVERAGE, row(field("ts", dayToSecond())), CLASS_NAME));
  }

  @Test
  void anIntervalForAMessageThatIsNotADurationFailsToRead() {
    final Descriptor inner = COVERAGE.findFieldByName("inner").getMessageType();
    final Message message =
        DynamicMessage.newBuilder(COVERAGE)
            .setField(
                COVERAGE.findFieldByName("inner"),
                DynamicMessage.newBuilder(inner)
                    .setField(inner.findFieldByName("inner_name"), "x")
                    .build())
            .build();
    final IllegalStateException e =
        assertThrows(
            IllegalStateException.class, () -> read(row(field("inner", dayToSecond())), message));
    assertTrue(e.getMessage().contains("inner_name"), e.getMessage());
  }
}
