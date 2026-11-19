package com.bbrownsound.flink.formats.proto.registry.confluent.bench;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestNestedProjection;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeatedNested;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import org.apache.flink.table.types.logical.RowType;

/**
 * The test messages the benchmarks run over, each with one fully populated sample record. A JMH
 * {@code @Param} names a constant of this enum, so a new message is one more constant.
 */
public enum BenchMessage {
  /** Two short strings. */
  SIMPLE {
    @Override
    public Message sample() {
      return TestSimple.SimpleMessage.newBuilder()
          .setContent("order-1234 shipped to warehouse 7")
          .setDateTime("2026-11-18T10:15:30Z")
          .build();
    }
  },

  /** 28 scalar fields of every protobuf scalar type. */
  BIG {
    @Override
    public Message sample() {
      return TestBig.BigPbMessage.newBuilder()
          .setIntField1(42)
          .setBoolField2(true)
          .setStringField3("string field three")
          .setBytesField4(ByteString.copyFromUtf8("bytes field four"))
          .setDoubleField5(5.5d)
          .setFloatField6(2.718f)
          .setUint32Field7(7)
          .setInt64Field8(8_000_000_000L)
          .setUint64Field9(9_000_000_000L)
          .setBytesField10(ByteString.copyFromUtf8("bytes field ten"))
          .setDoubleField11(11.11d)
          .setBytesField12(ByteString.copyFromUtf8("bytes field twelve"))
          .setBoolField13(true)
          .setStringField14("string field fourteen")
          .setFloatField15(15.5f)
          .setInt32Field16(-16)
          .setBytesField17(ByteString.copyFromUtf8("bytes field seventeen"))
          .setBoolField18(false)
          .setStringField19("string field nineteen")
          .setFloatField20(20.25f)
          .setFixed32Field21(21)
          .setFixed64Field22(22L)
          .setSfixed32Field23(-23)
          .setSfixed64Field24(-24L)
          .setDoubleField25(25.025d)
          .setUint32Field26(26)
          .setUint64Field27(27L)
          .setBoolField28(true)
          .build();
    }
  },

  /** A nested message with two optional scalars. */
  REPEATED_NESTED {
    @Override
    public Message sample() {
      return TestRepeatedNested.RepeatedMessageTest.newBuilder()
          .setD(
              TestRepeatedNested.RepeatedMessageTest.InnerMessageTest.newBuilder().setA(1).setB(2L))
          .build();
    }
  },

  /** A string map and a map of nested messages, five entries each. */
  MAP {
    @Override
    public Message sample() {
      TestMap.MapTest.Builder builder = TestMap.MapTest.newBuilder();
      for (int i = 0; i < 5; i++) {
        builder.putSimpleStringMap("key-" + i, "value-" + i);
        builder.putNestedMap(
            "nested-" + i, TestMap.NestedMap.newBuilder().setId("id-" + i).build());
      }
      return builder.build();
    }

    @Override
    public boolean pinMessageClass() {
      // MapTest is the second message in test_map.proto, after NestedMap.
      return true;
    }
  },

  /**
   * A wide nested message: a {@code payload} of 24 fields (scalars, a nested message, a oneof, a
   * repeated field and a map) next to an ID, a oneof, a wrapper and a repeated message.
   */
  NESTED {
    @Override
    public Message sample() {
      TestNestedProjection.NestedProjectionTest.Inner inner =
          TestNestedProjection.NestedProjectionTest.Inner.newBuilder()
              .setX("inner")
              .setY(7)
              .build();
      return TestNestedProjection.NestedProjectionTest.newBuilder()
          .setId("order-1234")
          .setPayload(
              TestNestedProjection.NestedProjectionTest.Payload.newBuilder()
                  .setA(42L)
                  .setB("payload b")
                  .setC(3)
                  .setInner(inner)
                  .setText("payload text")
                  .setF7("field seven")
                  .setF8("field eight")
                  .setF9("field nine")
                  .setF10("field ten")
                  .setF11(11L)
                  .setF12(12L)
                  .setF13(13L)
                  .setF14(14L)
                  .setF15(15.5d)
                  .setF16(16.5d)
                  .setF17(17.5d)
                  .setF18(18.5d)
                  .setF19(true)
                  .setF20(false)
                  .setF21(ByteString.copyFromUtf8("field twenty-one"))
                  .setF22(ByteString.copyFromUtf8("field twenty-two"))
                  .addF23("one")
                  .addF23("two")
                  .putF24("k1", "v1")
                  .putF24("k2", "v2"))
          .setCode(5)
          .setNote(StringValue.of("a note"))
          .addItems(inner)
          .build();
    }
  };

  /**
   * The sample record.
   *
   * @return a fully populated message
   */
  public abstract Message sample();

  /**
   * Whether the table must pin {@code message-class}: true when the message is not the first one in
   * its schema file, which is the one the sink otherwise serializes against.
   *
   * @return whether to set {@code message-class}
   */
  public boolean pinMessageClass() {
    return false;
  }

  /**
   * The table row type for this message, as the format derives it from the descriptor.
   *
   * @return the full row type, every field a column
   */
  public RowType rowType() {
    return (RowType) ProtoToLogicalType.toLogicalType(sample().getDescriptorForType());
  }
}
