package com.bbrownsound.flink.formats.proto.registry.confluent;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoRowDataDeserializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestEnums;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeatedNested;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.bbrownsound.flink.formats.proto.test.v1.TestWrapperTypes;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ProtoToRowTest extends ProtoConfluentTestBase {

  @Test
  public void testProtoToRow() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var deserializer =
        new ProtoRowDataDeserializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor()),
            null,
            new ProtoConfluentFormatConfig(url, "simple", false, config));
    deserializer.open(null);

    byte[] protoBytes =
        serializeMessage(
            TestSimple.SimpleMessage.newBuilder().setContent("test").setDateTime("date").build(),
            "simple");

    RowData data = deserializer.deserialize(protoBytes);
    StringData field1 = data.getString(0);
    StringData field2 = data.getString(1);

    Assertions.assertEquals("test", field1.toString());
    Assertions.assertEquals("date", field2.toString());
  }

  @Test
  public void testBigProtoToRow() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var deserializer =
        new ProtoRowDataDeserializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestBig.BigPbMessage.getDescriptor()),
            null,
            new ProtoConfluentFormatConfig(url, "big_proto", false, config));
    deserializer.open(null);

    byte[] protoBytes =
        serializeMessage(
            TestBig.BigPbMessage.newBuilder()
                .setIntField1(1)
                .setBoolField2(true)
                .setStringField3("test")
                .setBytesField4(ByteString.copyFromUtf8("4"))
                .setDoubleField5(20)
                .setFloatField6(21)
                .setUint32Field7(40)
                .setInt64Field8(60)
                .setUint64Field9(80)
                .setBytesField10(ByteString.copyFromUtf8("10"))
                .setDoubleField11(11)
                .setBytesField12(ByteString.copyFromUtf8("12"))
                .setBoolField13(false)
                .setStringField14("14string")
                .setFloatField15(15)
                .setInt32Field16(16)
                .setBytesField17(ByteString.copyFromUtf8("17"))
                .build(),
            "big_proto");

    RowData data = deserializer.deserialize(protoBytes);
    Assertions.assertEquals(1, data.getInt(0));
    Assertions.assertTrue(data.getBoolean(1));
    Assertions.assertEquals("test", data.getString(2).toString());
    Assertions.assertArrayEquals("4".getBytes(StandardCharsets.UTF_8), data.getBinary(3));
    Assertions.assertEquals(20, data.getDouble(4));
    Assertions.assertEquals(21f, data.getFloat(5));
    Assertions.assertEquals(40L, data.getLong(6));
    Assertions.assertEquals(60L, data.getLong(7));
    Assertions.assertEquals(80L, data.getLong(8));
    Assertions.assertArrayEquals("10".getBytes(StandardCharsets.UTF_8), data.getBinary(9));
    Assertions.assertEquals(11, data.getDouble(10));
    Assertions.assertArrayEquals("12".getBytes(StandardCharsets.UTF_8), data.getBinary(11));
    Assertions.assertFalse(data.getBoolean(12));
    Assertions.assertEquals("14string", data.getString(13).toString());
    Assertions.assertEquals(15f, data.getFloat(14));
    Assertions.assertEquals(16, data.getInt(15));
    Assertions.assertArrayEquals("17".getBytes(StandardCharsets.UTF_8), data.getBinary(16));
  }

  @Test
  public void testWrapperTypes() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var topic = "wrapper";
    var deserializer =
        new ProtoRowDataDeserializationSchema(
            (RowType)
                ProtoToLogicalType.toLogicalType(TestWrapperTypes.WrapperTypesTest.getDescriptor()),
            null,
            new ProtoConfluentFormatConfig(url, topic, false, config));
    deserializer.open(null);

    byte[] protoBytes =
        serializeMessage(
            TestWrapperTypes.WrapperTypesTest.newBuilder()
                .setInt32Value(Int32Value.newBuilder().setValue(1).build())
                .setInt64Value(Int64Value.newBuilder().setValue(2).build())
                .setUint32Value(UInt32Value.newBuilder().setValue(3).build())
                .setUint64Value(UInt64Value.newBuilder().setValue(4).build())
                .setFloatValue(FloatValue.newBuilder().setValue(5).build())
                .setDoubleValue(DoubleValue.newBuilder().setValue(6).build())
                .setBoolValue(BoolValue.newBuilder().setValue(true).build())
                .setStringValue(StringValue.newBuilder().setValue("test").build())
                .setBytesValue(
                    BytesValue.newBuilder().setValue(ByteString.copyFromUtf8("bytes")).build())
                .build(),
            topic);

    RowData data = deserializer.deserialize(protoBytes);
    Assertions.assertEquals(1, data.getInt(0));
    Assertions.assertEquals(2L, data.getLong(1));
    Assertions.assertEquals(3L, data.getLong(2));
    Assertions.assertEquals(4L, data.getLong(3));
    Assertions.assertEquals(5f, data.getFloat(4));
    Assertions.assertEquals(6.0, data.getDouble(5));
    Assertions.assertTrue(data.getBoolean(6));
    Assertions.assertEquals("test", data.getString(7).toString());
    Assertions.assertArrayEquals("bytes".getBytes(StandardCharsets.UTF_8), data.getBinary(8));
  }

  @Test
  public void testMap() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var topic = "map";
    var deserializer =
        new ProtoRowDataDeserializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestMap.MapTest.getDescriptor()),
            null,
            new ProtoConfluentFormatConfig(url, topic, false, config));
    deserializer.open(null);

    byte[] protoBytes =
        serializeMessage(
            TestMap.MapTest.newBuilder()
                .putAllSimpleStringMap(Map.of("key1", "value1"))
                .putAllNestedMap(
                    Map.of("key1", TestMap.NestedMap.newBuilder().setId("bla").build()))
                .build(),
            topic);

    RowData data = deserializer.deserialize(protoBytes);
    ArrayData keys = data.getMap(0).keyArray();
    ArrayData values = data.getMap(0).valueArray();

    Assertions.assertEquals("key1", keys.getString(0).toString());
    Assertions.assertEquals("value1", values.getString(0).toString());

    ArrayData keysNested = data.getMap(1).keyArray();
    ArrayData valuesNested = data.getMap(1).valueArray();

    Assertions.assertEquals("key1", keysNested.getString(0).toString());
    Assertions.assertEquals("bla", valuesNested.getRow(0, 1).getString(0).toString());
  }

  @Test
  public void testRepeated() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var topic = "repeated";
    var deserializer =
        new ProtoRowDataDeserializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestRepeated.RepeatedTest.getDescriptor()),
            null,
            new ProtoConfluentFormatConfig(url, topic, false, config));
    deserializer.open(null);

    byte[] protoBytes =
        serializeMessage(
            TestRepeated.RepeatedTest.newBuilder()
                .addAllValues(List.of("1", "2", "3"))
                .addAllNumbers(List.of(1, 2, 3))
                .addAllFloats(List.of(1f, 2f, 3f))
                .addAllDoubles(List.of(1d, 2d, 3d))
                .addAllBools(List.of(true, false, true))
                .addAllBytes(
                    List.of(
                        ByteString.copyFromUtf8("1"),
                        ByteString.copyFromUtf8("2"),
                        ByteString.copyFromUtf8("3")))
                .addAllStringValues(
                    List.of(
                        StringValue.newBuilder().setValue("1").build(),
                        StringValue.newBuilder().setValue("2").build(),
                        StringValue.newBuilder().setValue("3").build()))
                .addAllTimestamps(
                    List.of(
                        Timestamp.newBuilder().setSeconds(1).setNanos(1).build(),
                        Timestamp.newBuilder().setSeconds(2).setNanos(2).build(),
                        Timestamp.newBuilder().setSeconds(3).setNanos(3).build()))
                .addAllNestedRepeated(
                    List.of(
                        TestRepeated.NestedRepeated.newBuilder().setValue("test").build(),
                        TestRepeated.NestedRepeated.newBuilder().setValue("test2").build()))
                .build(),
            topic);

    RowData data = deserializer.deserialize(protoBytes);
    ArrayData values = data.getArray(0);
    Assertions.assertEquals("1", values.getString(0).toString());
    Assertions.assertEquals("2", values.getString(1).toString());
    Assertions.assertEquals("3", values.getString(2).toString());

    ArrayData numbers = data.getArray(1);
    Assertions.assertEquals(1, numbers.getInt(0));
    Assertions.assertEquals(2, numbers.getInt(1));
    Assertions.assertEquals(3, numbers.getInt(2));

    ArrayData floats = data.getArray(2);
    Assertions.assertEquals(1f, floats.getFloat(0));
    Assertions.assertEquals(2f, floats.getFloat(1));
    Assertions.assertEquals(3f, floats.getFloat(2));

    ArrayData doubles = data.getArray(3);
    Assertions.assertEquals(1d, doubles.getDouble(0));
    Assertions.assertEquals(2d, doubles.getDouble(1));
    Assertions.assertEquals(3d, doubles.getDouble(2));

    ArrayData bools = data.getArray(4);
    Assertions.assertTrue(bools.getBoolean(0));
    Assertions.assertFalse(bools.getBoolean(1));
    Assertions.assertTrue(bools.getBoolean(2));

    ArrayData bytes = data.getArray(5);
    Assertions.assertArrayEquals("1".getBytes(StandardCharsets.UTF_8), bytes.getBinary(0));
    Assertions.assertArrayEquals("2".getBytes(StandardCharsets.UTF_8), bytes.getBinary(1));
    Assertions.assertArrayEquals("3".getBytes(StandardCharsets.UTF_8), bytes.getBinary(2));

    ArrayData stringValues = data.getArray(6);
    Assertions.assertEquals("1", stringValues.getString(0).toString());
    Assertions.assertEquals("2", stringValues.getString(1).toString());
    Assertions.assertEquals("3", stringValues.getString(2).toString());

    ArrayData timestamps = data.getArray(7);
    Assertions.assertEquals(1000, timestamps.getTimestamp(0, 0).getMillisecond());
    Assertions.assertEquals(2000, timestamps.getTimestamp(1, 0).getMillisecond());
    Assertions.assertEquals(3000, timestamps.getTimestamp(2, 0).getMillisecond());

    ArrayData nestedRepeated = data.getArray(8);
    Assertions.assertEquals("test", nestedRepeated.getRow(0, 1).getString(0).toString());
    Assertions.assertEquals("test2", nestedRepeated.getRow(1, 1).getString(0).toString());
  }

  @Test
  public void testRepeatedNested() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var topic = "nested-repeated";
    var deserializer =
        new ProtoRowDataDeserializationSchema(
            (RowType)
                ProtoToLogicalType.toLogicalType(
                    TestRepeatedNested.RepeatedMessageTest.getDescriptor()),
            null,
            new ProtoConfluentFormatConfig(url, topic, false, config));
    deserializer.open(null);

    byte[] protoBytes =
        serializeMessage(
            TestRepeatedNested.RepeatedMessageTest.newBuilder()
                .setD(
                    TestRepeatedNested.RepeatedMessageTest.InnerMessageTest.newBuilder()
                        .setA(1)
                        .setB(2)
                        .build())
                .build(),
            topic);

    RowData data = deserializer.deserialize(protoBytes);
    RowData innerMessage = data.getRow(0, 2);
    Assertions.assertEquals(1, innerMessage.getInt(0));
    Assertions.assertEquals(2L, innerMessage.getLong(1));
  }

  @Test
  public void testEnums() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var topic = "enums";
    var deserializer =
        new ProtoRowDataDeserializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestEnums.TestEnum.getDescriptor()),
            null,
            new ProtoConfluentFormatConfig(url, topic, false, config));
    deserializer.open(null);

    byte[] protoBytes =
        serializeMessage(
            TestEnums.TestEnum.newBuilder()
                .setType(TestEnums.MessageType.TEXT)
                .addAllStatus(
                    List.of(TestEnums.MessageStatus.READ, TestEnums.MessageStatus.RECEIVED))
                .putStatusMap("key", TestEnums.MessageStatus.READ)
                .build(),
            topic);

    RowData data = deserializer.deserialize(protoBytes);
    Assertions.assertEquals("TEXT", data.getString(0).toString());
    ArrayData status = data.getArray(1);
    Assertions.assertEquals("READ", status.getString(0).toString());
    Assertions.assertEquals("RECEIVED", status.getString(1).toString());
    ArrayData statusMapKeys = data.getMap(2).keyArray();
    ArrayData statusMapValues = data.getMap(2).valueArray();
    Assertions.assertEquals("key", statusMapKeys.getString(0).toString());
    Assertions.assertEquals("READ", statusMapValues.getString(0).toString());
  }
}
