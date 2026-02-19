package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.List;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class RowTypeToProtoTest {

  @Test
  void fromRowType_simpleRow() {
    RowType rowType =
        new RowType(
            false,
            List.of(
                new RowType.RowField("name", new VarCharType(false, 100)),
                new RowType.RowField("value", new VarCharType(true, 255))));
    Descriptor descriptor = RowTypeToProto.fromRowType(rowType, "SimpleRow", "test.v1");
    assertNotNull(descriptor);
    assertEquals("SimpleRow", descriptor.getName());
    assertEquals(2, descriptor.getFields().size());
    assertEquals("name", descriptor.getFields().get(0).getName());
    assertEquals("value", descriptor.getFields().get(1).getName());
  }

  @Test
  void fromRowType_withArray() {
    RowType rowType =
        new RowType(
            false, List.of(new RowType.RowField("ids", new ArrayType(false, new IntType(false)))));
    Descriptor descriptor = RowTypeToProto.fromRowType(rowType, "WithArray", "test.v1");
    assertNotNull(descriptor);
    assertEquals(1, descriptor.getFields().size());
    assertTrue(descriptor.getFields().get(0).isRepeated());
  }

  @Test
  void fromRowType_withMap() {
    RowType rowType =
        new RowType(
            false,
            List.of(
                new RowType.RowField(
                    "tags",
                    new MapType(false, new VarCharType(false, 100), new VarCharType(false, 255)))));
    Descriptor descriptor = RowTypeToProto.fromRowType(rowType, "WithMap", "test.v1");
    assertNotNull(descriptor);
    assertEquals(1, descriptor.getFields().size());
  }

  @Test
  void fromRowType_withDecimal() {
    RowType rowType =
        new RowType(false, List.of(new RowType.RowField("amount", new DecimalType(10, 2))));
    Descriptor descriptor = RowTypeToProto.fromRowType(rowType, "WithDecimal", "test.v1");
    assertNotNull(descriptor);
    assertEquals(1, descriptor.getFields().size());
  }

  @Test
  void fromRowType_nestedRow() {
    RowType inner =
        new RowType(
            false,
            List.of(
                new RowType.RowField("x", new IntType(false)),
                new RowType.RowField("y", new VarCharType(false, 50))));
    RowType rowType =
        new RowType(
            false,
            List.of(
                new RowType.RowField("name", new VarCharType(false, 100)),
                new RowType.RowField("inner", inner)));
    Descriptor descriptor = RowTypeToProto.fromRowType(rowType, "OuterRow", "test.v1");
    assertNotNull(descriptor);
    assertEquals(2, descriptor.getFields().size());
    assertEquals(1, descriptor.getNestedTypes().size());
    Descriptor nested = descriptor.getNestedTypes().get(0);
    assertEquals("inner_Row", nested.getName());
    assertEquals(2, nested.getFields().size());
  }

  @Test
  void fromRowType_timestampDateTime() {
    RowType rowType =
        new RowType(
            false,
            List.of(
                new RowType.RowField("ts", new LocalZonedTimestampType(false, 3)),
                new RowType.RowField("d", new DateType(false)),
                new RowType.RowField("t", new TimeType(false, 0))));
    Descriptor descriptor = RowTypeToProto.fromRowType(rowType, "WithTimeTypes", "test.v1");
    assertNotNull(descriptor);
    assertEquals(3, descriptor.getFields().size());
  }

  @Test
  void fromRowType_tinyIntAndSmallInt() {
    RowType rowType =
        new RowType(
            false,
            List.of(
                new RowType.RowField("tiny", new TinyIntType(false)),
                new RowType.RowField("small", new SmallIntType(false)),
                new RowType.RowField("big", new BigIntType(false))));
    Descriptor descriptor = RowTypeToProto.fromRowType(rowType, "WithIntSizes", "test.v1");
    assertNotNull(descriptor);
    assertEquals(3, descriptor.getFields().size());
  }

  @Test
  void fromRowType_rowNameAndPackageVariations() {
    RowType rowType =
        new RowType(false, List.of(new RowType.RowField("a", new VarCharType(false, 10))));
    Descriptor d1 = RowTypeToProto.fromRowType(rowType, "CustomName", "com.example.pkg");
    assertNotNull(d1);
    assertEquals("CustomName", d1.getName());
    assertEquals("com.example.pkg", d1.getFile().getPackage());
  }
}
