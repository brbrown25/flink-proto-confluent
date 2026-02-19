package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RowTypeToProto;
import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestEnums;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestOneof;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.bbrownsound.flink.formats.proto.test.v1.TestWrapperTypes;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Map;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.junit.jupiter.api.Test;

class RowDataToProtoConvertersTest {

  @Test
  void simpleMessage_rowToProto() {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    var converter =
        RowDataToProtoConverters.createConverter(rowType, TestSimple.SimpleMessage.getDescriptor());
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("hello"));
    row.setField(1, StringData.fromString("2024-01-01"));
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    assertEquals("hello", msg.getField(msg.getDescriptorForType().findFieldByName("content")));
    assertEquals(
        "2024-01-01", msg.getField(msg.getDescriptorForType().findFieldByName("date_time")));
  }

  @Test
  void bigPbMessage_rowToProto() {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestBig.BigPbMessage.getDescriptor());
    var converter =
        RowDataToProtoConverters.createConverter(rowType, TestBig.BigPbMessage.getDescriptor());
    GenericRowData row = new GenericRowData(28);
    row.setField(0, 1);
    row.setField(1, true);
    row.setField(2, StringData.fromString("s"));
    row.setField(3, "bytes".getBytes(StandardCharsets.UTF_8));
    row.setField(4, 20.0);
    row.setField(5, 21f);
    row.setField(6, 40L);
    row.setField(7, 60L);
    row.setField(8, 80L);
    row.setField(9, "10".getBytes(StandardCharsets.UTF_8));
    row.setField(10, 11.0);
    row.setField(11, "12".getBytes(StandardCharsets.UTF_8));
    row.setField(12, false);
    row.setField(13, StringData.fromString("14"));
    row.setField(14, 15f);
    row.setField(15, 16);
    row.setField(16, "17".getBytes(StandardCharsets.UTF_8));
    row.setField(17, true);
    row.setField(18, StringData.fromString("19"));
    row.setField(19, 20f);
    row.setField(20, 21L);
    row.setField(21, 22L);
    row.setField(22, 23);
    row.setField(23, 24L);
    row.setField(24, 25.0);
    row.setField(25, 26L);
    row.setField(26, 27L);
    row.setField(27, false);
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    assertEquals(1, msg.getField(msg.getDescriptorForType().findFieldByName("int_field_1")));
    assertEquals("s", msg.getField(msg.getDescriptorForType().findFieldByName("string_field_3")));
  }

  @Test
  void wrapperTypes_rowToProto() {
    RowType rowType =
        (RowType)
            ProtoToLogicalType.toLogicalType(TestWrapperTypes.WrapperTypesTest.getDescriptor());
    var converter =
        RowDataToProtoConverters.createConverter(
            rowType, TestWrapperTypes.WrapperTypesTest.getDescriptor());
    GenericRowData row = new GenericRowData(9);
    row.setField(0, 1);
    row.setField(1, 2L);
    row.setField(2, 3L);
    row.setField(3, 4L);
    row.setField(4, 5f);
    row.setField(5, 6.0);
    row.setField(6, true);
    row.setField(7, StringData.fromString("test"));
    row.setField(8, "bytes".getBytes(StandardCharsets.UTF_8));
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    assertNotNull(msg.getField(msg.getDescriptorForType().findFieldByName("int32Value")));
    assertNotNull(msg.getField(msg.getDescriptorForType().findFieldByName("stringValue")));
  }

  @Test
  void repeatedTest_rowToProto() {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestRepeated.RepeatedTest.getDescriptor());
    var converter =
        RowDataToProtoConverters.createConverter(
            rowType, TestRepeated.RepeatedTest.getDescriptor());
    GenericRowData row = new GenericRowData(9);
    row.setField(
        0,
        new GenericArrayData(
            new Object[] {StringData.fromString("a"), StringData.fromString("b")}));
    row.setField(1, new GenericArrayData(new int[] {1, 2}));
    row.setField(2, new GenericArrayData(new Float[] {1f, 2f}));
    row.setField(3, new GenericArrayData(new Double[] {1d, 2d}));
    row.setField(4, new GenericArrayData(new Boolean[] {true, false}));
    row.setField(
        5,
        new GenericArrayData(
            new byte[][] {
              "x".getBytes(StandardCharsets.UTF_8), "y".getBytes(StandardCharsets.UTF_8)
            }));
    row.setField(
        6,
        new GenericArrayData(
            new Object[] {StringData.fromString("1"), StringData.fromString("2")}));
    row.setField(
        7,
        new GenericArrayData(
            new Object[] {
              TimestampData.fromEpochMillis(1000L, 0), TimestampData.fromEpochMillis(2000L, 0)
            }));
    row.setField(8, null);
    Object result = converter.convert(row);
    assertNotNull(result);
  }

  @Test
  void mapTest_rowToProto() {
    RowType rowType = (RowType) ProtoToLogicalType.toLogicalType(TestMap.MapTest.getDescriptor());
    var converter =
        RowDataToProtoConverters.createConverter(rowType, TestMap.MapTest.getDescriptor());
    GenericMapData simpleMap =
        new GenericMapData(Map.of(StringData.fromString("k1"), StringData.fromString("v1")));
    GenericRowData nestedRow = new GenericRowData(1);
    nestedRow.setField(0, StringData.fromString("id1"));
    GenericMapData nestedMap = new GenericMapData(Map.of(StringData.fromString("nk"), nestedRow));
    GenericRowData row = new GenericRowData(2);
    row.setField(0, simpleMap);
    row.setField(1, nestedMap);
    Object result = converter.convert(row);
    assertNotNull(result);
  }

  @Test
  void date_rowToProto() {
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(new RowType.RowField("d", new DateType(true))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "WithDate", "test.v1");
    var converter = RowDataToProtoConverters.createConverter(rowType, descriptor);
    GenericRowData row = new GenericRowData(1);
    row.setField(0, DateTimeUtils.toInternal(LocalDate.of(2024, 1, 15)));
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    Date d = (Date) msg.getField(descriptor.findFieldByName("d"));
    assertEquals(2024, d.getYear());
    assertEquals(1, d.getMonth());
    assertEquals(15, d.getDay());
  }

  @Test
  void time_rowToProto() {
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(new RowType.RowField("t", new TimeType(false, 0))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "WithTime", "test.v1");
    var converter = RowDataToProtoConverters.createConverter(rowType, descriptor);
    GenericRowData row = new GenericRowData(1);
    row.setField(0, 3723000);
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    TimeOfDay t = (TimeOfDay) msg.getField(descriptor.findFieldByName("t"));
    assertEquals(1, t.getHours());
    assertEquals(2, t.getMinutes());
    assertEquals(3, t.getSeconds());
  }

  @Test
  void decimal_rowToProto() {
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(new RowType.RowField("dec", new DecimalType(10, 2))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "WithDecimal", "test.v1");
    var converter = RowDataToProtoConverters.createConverter(rowType, descriptor);
    GenericRowData row = new GenericRowData(1);
    row.setField(0, DecimalData.fromBigDecimal(new java.math.BigDecimal("99.99"), 10, 2));
    Object result = converter.convert(row);
    assertNotNull(result);
  }

  @Test
  void enum_rowToProto() {
    RowType rowType =
        (RowType)
            ProtoToLogicalType.toLogicalType(TestEnums.TestEnum.getDescriptor());
    var converter =
        RowDataToProtoConverters.createConverter(
            rowType, TestEnums.TestEnum.getDescriptor());
    GenericRowData row = new GenericRowData(3);
    row.setField(0, StringData.fromString("VIDEO"));
    row.setField(1, null);
    row.setField(2, null);
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    assertEquals(
        "VIDEO",
        msg.getField(msg.getDescriptorForType().findFieldByName("type")).toString());
  }

  @Test
  void oneof_rowToProto() {
    Descriptor descriptor = TestOneof.MessageWithOneof.getDescriptor();
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(descriptor);
    var converter = RowDataToProtoConverters.createConverter(rowType, descriptor);
    GenericRowData choiceRow = new GenericRowData(2);
    choiceRow.setField(0, 42);
    choiceRow.setField(1, null);
    GenericRowData row = new GenericRowData(1);
    row.setField(0, choiceRow);
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    assertEquals(42, msg.getField(descriptor.findFieldByName("a")));
  }

  @Test
  void oneof_rowToProto_stringBranch() {
    Descriptor descriptor = TestOneof.MessageWithOneof.getDescriptor();
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(descriptor);
    var converter = RowDataToProtoConverters.createConverter(rowType, descriptor);
    GenericRowData choiceRow = new GenericRowData(2);
    choiceRow.setField(0, null);
    choiceRow.setField(1, StringData.fromString("hello"));
    GenericRowData row = new GenericRowData(1);
    row.setField(0, choiceRow);
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    assertEquals("hello", msg.getField(descriptor.findFieldByName("b")));
  }

  @Test
  void nullOptionalField_skipsSetField() {
    RowType rowType =
        new RowType(
            true,
            java.util.List.of(
                new RowType.RowField("a", new VarCharType(true, 100)),
                new RowType.RowField("b", new VarCharType(true, 100))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "TwoOptional", "test.v1");
    var converter = RowDataToProtoConverters.createConverter(rowType, descriptor);
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("only_a"));
    row.setField(1, null);
    Object result = converter.convert(row);
    assertNotNull(result);
    DynamicMessage msg = (DynamicMessage) result;
    assertEquals("only_a", msg.getField(descriptor.findFieldByName("a")).toString());
  }

  @Test
  void multiset_rowToProto() {
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(
                new RowType.RowField(
                    "counts",
                    new MultisetType(false, new VarCharType(false, 50)))));
    var descriptor = RowTypeToProto.fromRowType(rowType, "WithMultiset", "test.v1");
    var converter = RowDataToProtoConverters.createConverter(rowType, descriptor);
    GenericMapData multiset = new GenericMapData(Map.of(StringData.fromString("x"), 2));
    GenericRowData row = new GenericRowData(1);
    row.setField(0, multiset);
    Object result = converter.convert(row);
    assertNotNull(result);
  }

  @Test
  void unsupportedType_throws() {
    org.apache.flink.table.types.logical.TimestampType timestampType =
        new org.apache.flink.table.types.logical.TimestampType(3);
    RowType rowType =
        new RowType(
            false,
            java.util.List.of(new RowType.RowField("x", timestampType)));
    var descriptor =
        RowTypeToProto.fromRowType(
            new RowType(
                false,
                java.util.List.of(
                    new RowType.RowField("x", new VarCharType(false, 10)))),
            "Dummy",
            "test.v1");
    assertThrows(
        UnsupportedOperationException.class,
        () -> RowDataToProtoConverters.createConverter(rowType, descriptor));
  }
}
