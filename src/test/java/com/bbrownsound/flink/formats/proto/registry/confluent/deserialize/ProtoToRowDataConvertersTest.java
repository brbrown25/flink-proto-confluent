package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RowTypeToProto;
import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestEnums;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestOneof;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeatedNested;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.bbrownsound.flink.formats.proto.test.v1.TestWrapperTypes;
import com.google.protobuf.StringValue;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.confluent.protobuf.type.Decimal;
import org.junit.jupiter.api.Test;

class ProtoToRowDataConvertersTest {

  @Test
  void simpleMessage_protoToRow() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(TestSimple.SimpleMessage.getDescriptor(), rowType);
    TestSimple.SimpleMessage msg =
        TestSimple.SimpleMessage.newBuilder().setContent("hello").setDateTime("2024-01-01").build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(StringData.fromString("hello"), row.getString(0));
    assertEquals(StringData.fromString("2024-01-01"), row.getString(1));
  }

  @Test
  void bigPbMessage_protoToRow() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestBig.BigPbMessage.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(TestBig.BigPbMessage.getDescriptor(), rowType);
    TestBig.BigPbMessage msg =
        TestBig.BigPbMessage.newBuilder()
            .setIntField1(1)
            .setBoolField2(true)
            .setStringField3("s")
            .setBytesField4(com.google.protobuf.ByteString.copyFrom("bytes".getBytes(StandardCharsets.UTF_8)))
            .setDoubleField5(20.0)
            .setFloatField6(21f)
            .setUint32Field7(40)
            .setInt64Field8(60L)
            .setUint64Field9(80L)
            .build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(1, row.getInt(0));
    assertTrue(row.getBoolean(1));
    assertEquals(StringData.fromString("s"), row.getString(2));
    assertEquals(20.0, row.getDouble(4));
    assertEquals(21f, row.getFloat(5));
    assertEquals(60L, row.getLong(7));
    assertEquals(80L, row.getLong(8));
  }

  @Test
  void simpleMessage_emptyProtoToRow() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(TestSimple.SimpleMessage.getDescriptor(), rowType);
    TestSimple.SimpleMessage msg = TestSimple.SimpleMessage.getDefaultInstance();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(StringData.fromString(""), row.getString(0));
    assertEquals(StringData.fromString(""), row.getString(1));
  }

  @Test
  void wrapperTypes_protoToRow() throws IOException {
    RowType rowType =
        (RowType)
            ProtoToLogicalType.toLogicalType(TestWrapperTypes.WrapperTypesTest.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(
            TestWrapperTypes.WrapperTypesTest.getDescriptor(), rowType);
    TestWrapperTypes.WrapperTypesTest msg =
        TestWrapperTypes.WrapperTypesTest.newBuilder()
            .setInt32Value(com.google.protobuf.Int32Value.of(1))
            .setStringValue(StringValue.of("test"))
            .build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(1, row.getInt(0));
    assertEquals(StringData.fromString("test"), row.getString(7));
  }

  @Test
  void repeatedTest_protoToRow() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestRepeated.RepeatedTest.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(
            TestRepeated.RepeatedTest.getDescriptor(), rowType);
    TestRepeated.RepeatedTest msg =
        TestRepeated.RepeatedTest.newBuilder()
            .addValues("a")
            .addValues("b")
            .addNumbers(1)
            .addNumbers(2)
            .addFloats(1f)
            .addDoubles(1d)
            .addBools(true)
            .addBytes(com.google.protobuf.ByteString.copyFrom("x".getBytes(StandardCharsets.UTF_8)))
            .addStringValues(StringValue.of("sv"))
            .build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    assertNotNull(((RowData) result).getArray(0));
    assertEquals(2, ((RowData) result).getArray(0).size());
  }

  @Test
  void mapTest_protoToRow() throws IOException {
    RowType rowType = (RowType) ProtoToLogicalType.toLogicalType(TestMap.MapTest.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(TestMap.MapTest.getDescriptor(), rowType);
    TestMap.MapTest msg =
        TestMap.MapTest.newBuilder()
            .putSimpleStringMap("k1", "v1")
            .putNestedMap("nk", TestMap.NestedMap.newBuilder().setId("id1").build())
            .build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertNotNull(row.getMap(0));
    assertNotNull(row.getMap(1));
  }

  @Test
  void repeatedMessageTest_protoToRow() throws IOException {
    RowType rowType =
        (RowType)
            ProtoToLogicalType.toLogicalType(
                TestRepeatedNested.RepeatedMessageTest.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(
            TestRepeatedNested.RepeatedMessageTest.getDescriptor(), rowType);
    TestRepeatedNested.RepeatedMessageTest msg =
        TestRepeatedNested.RepeatedMessageTest.newBuilder()
            .setD(
                TestRepeatedNested.RepeatedMessageTest.InnerMessageTest.newBuilder()
                    .setA(1)
                    .setB(2L)
                    .build())
            .build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertTrue(row.getArity() >= 1);
  }

  @Test
  void date_protoToRow() throws IOException {
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(new RowType.RowField("d", new DateType(true))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "WithDate", "test.v1");
    var converter = ProtoToRowDataConverters.createConverter(descriptor, rowType);
    Date dateMsg = Date.newBuilder().setYear(2024).setMonth(1).setDay(1).build();
    com.google.protobuf.DynamicMessage outer =
        com.google.protobuf.DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("d"), dateMsg)
            .build();
    Object result = converter.convert(outer);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(
        DateTimeUtils.toInternal(java.time.LocalDate.of(2024, 1, 1)), row.getInt(0));
  }

  @Test
  void time_protoToRow() throws IOException {
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(new RowType.RowField("t", new TimeType(false, 0))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "WithTime", "test.v1");
    var converter = ProtoToRowDataConverters.createConverter(descriptor, rowType);
    TimeOfDay timeMsg =
        TimeOfDay.newBuilder().setHours(1).setMinutes(2).setSeconds(3).setNanos(0).build();
    com.google.protobuf.DynamicMessage outer =
        com.google.protobuf.DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("t"), timeMsg)
            .build();
    Object result = converter.convert(outer);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(3723000, row.getInt(0));
  }

  @Test
  void decimal_protoToRow() throws IOException {
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(new RowType.RowField("dec", new DecimalType(10, 2))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "WithDecimal", "test.v1");
    var converter = ProtoToRowDataConverters.createConverter(descriptor, rowType);
    BigDecimal bd = new BigDecimal("123.45");
    Decimal decimalMsg =
        Decimal.newBuilder()
            .setValue(ByteString.copyFrom(bd.unscaledValue().toByteArray()))
            .build();
    com.google.protobuf.DynamicMessage outer =
        com.google.protobuf.DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("dec"), decimalMsg)
            .build();
    Object result = converter.convert(outer);
    assertNotNull(result);
    RowData row = (RowData) result;
    DecimalData dec = row.getDecimal(0, 10, 2);
    assertEquals(0, bd.compareTo(dec.toBigDecimal()));
  }

  @Test
  void enum_protoToRow() throws IOException {
    RowType rowType =
        (RowType)
            ProtoToLogicalType.toLogicalType(TestEnums.TestEnum.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(
            TestEnums.TestEnum.getDescriptor(), rowType);
    TestEnums.TestEnum msg =
        TestEnums.TestEnum.newBuilder()
            .setType(TestEnums.MessageType.IMAGE)
            .build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(StringData.fromString("IMAGE"), row.getString(0));
  }

  @Test
  void oneof_protoToRow() throws IOException {
    RowType rowType =
        (RowType)
            ProtoToLogicalType.toLogicalType(
                TestOneof.MessageWithOneof.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(
            TestOneof.MessageWithOneof.getDescriptor(), rowType);
    TestOneof.MessageWithOneof msg =
        TestOneof.MessageWithOneof.newBuilder().setA(42).build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    RowData choiceRow = row.getRow(0, 2);
    assertNotNull(choiceRow);
    assertEquals(42, choiceRow.getInt(0));
  }

  @Test
  void oneof_protoToRow_stringBranch() throws IOException {
    RowType rowType =
        (RowType)
            ProtoToLogicalType.toLogicalType(
                TestOneof.MessageWithOneof.getDescriptor());
    var converter =
        ProtoToRowDataConverters.createConverter(
            TestOneof.MessageWithOneof.getDescriptor(), rowType);
    TestOneof.MessageWithOneof msg =
        TestOneof.MessageWithOneof.newBuilder().setB("hello").build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    RowData choiceRow = row.getRow(0, 2);
    assertNotNull(choiceRow);
    assertEquals(StringData.fromString("hello"), choiceRow.getString(1));
  }

  @Test
  void snakeCaseFieldName_protoToRow() throws IOException {
    var descriptor =
        RowTypeToProto.fromRowType(
            new RowType(
                false,
                java.util.List.of(
                    new RowType.RowField("myField", new VarCharType(false, 100)))),
            "CamelCaseMessage",
            "test.v1");
    RowType rowTypeWithSnake =
        new RowType(
            false,
            java.util.List.of(
                new RowType.RowField("my_field", new VarCharType(false, 100))));
    var converter = ProtoToRowDataConverters.createConverter(descriptor, rowTypeWithSnake);
    com.google.protobuf.DynamicMessage msg =
        com.google.protobuf.DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("myField"), "hello")
            .build();
    Object result = converter.convert(msg);
    assertNotNull(result);
    RowData row = (RowData) result;
    assertEquals(StringData.fromString("hello"), row.getString(0));
  }

  @Test
  void tableFieldNotFound_throws() {
    RowType rowTypeWithExtra =
        new RowType(
            false,
            java.util.List.of(
                new RowType.RowField("content", new VarCharType(false, 100)),
                new RowType.RowField("missing_field", new VarCharType(false, 100))));
    var descriptor =
        RowTypeToProto.fromRowType(
            new RowType(
                false,
                java.util.List.of(
                    new RowType.RowField("content", new VarCharType(false, 100)))),
            "OneField",
            "test.v1");
    assertThrows(
        IllegalStateException.class,
        () -> ProtoToRowDataConverters.createConverter(descriptor, rowTypeWithExtra));
  }
}
