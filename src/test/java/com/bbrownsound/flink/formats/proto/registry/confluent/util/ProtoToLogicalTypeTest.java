package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeatedNested;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.bbrownsound.flink.formats.proto.test.v1.TestWrapperTypes;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class ProtoToLogicalTypeTest {

  @Test
  void toLogicalType_simpleMessage() {
    LogicalType type = ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    assertNotNull(type);
    assertTrue(type instanceof RowType);
    RowType rowType = (RowType) type;
    assertEquals(2, rowType.getFieldCount());
    assertEquals("content", rowType.getFields().get(0).getName());
    assertEquals("date_time", rowType.getFields().get(1).getName());
    assertTrue(rowType.getFields().get(0).getType() instanceof VarCharType);
    assertTrue(rowType.getFields().get(1).getType() instanceof VarCharType);
    assertFalse(rowType.isNullable());
  }

  @Test
  void toLogicalType_bigPbMessage() {
    LogicalType type = ProtoToLogicalType.toLogicalType(TestBig.BigPbMessage.getDescriptor());
    assertNotNull(type);
    assertTrue(type instanceof RowType);
    RowType rowType = (RowType) type;
    assertEquals(28, rowType.getFieldCount());
    assertTrue(rowType.getFields().get(0).getType() instanceof IntType);
    assertTrue(rowType.getFields().get(1).getType() instanceof BooleanType);
    assertTrue(rowType.getFields().get(2).getType() instanceof VarCharType);
    assertTrue(rowType.getFields().get(3).getType() instanceof VarBinaryType);
    assertTrue(rowType.getFields().get(4).getType() instanceof DoubleType);
    assertTrue(rowType.getFields().get(5).getType() instanceof FloatType);
    assertTrue(rowType.getFields().get(6).getType() instanceof BigIntType);
  }

  @Test
  void toLogicalType_wrapperTypesTest() {
    LogicalType type =
        ProtoToLogicalType.toLogicalType(TestWrapperTypes.WrapperTypesTest.getDescriptor());
    assertNotNull(type);
    assertTrue(type instanceof RowType);
    RowType rowType = (RowType) type;
    assertEquals(9, rowType.getFieldCount());
    assertTrue(rowType.getFields().get(0).getType() instanceof IntType);
    assertTrue(rowType.getFields().get(7).getType() instanceof VarCharType);
  }

  @Test
  void toLogicalType_repeatedTest() {
    LogicalType type = ProtoToLogicalType.toLogicalType(TestRepeated.RepeatedTest.getDescriptor());
    assertNotNull(type);
    assertTrue(type instanceof RowType);
    RowType rowType = (RowType) type;
    assertEquals(9, rowType.getFieldCount());
    assertTrue(rowType.getFields().get(0).getType() instanceof ArrayType);
    assertTrue(
        ((ArrayType) rowType.getFields().get(0).getType()).getElementType() instanceof VarCharType);
    assertTrue(rowType.getFields().get(1).getType() instanceof ArrayType);
  }

  @Test
  void toLogicalType_mapTest() {
    LogicalType type = ProtoToLogicalType.toLogicalType(TestMap.MapTest.getDescriptor());
    assertNotNull(type);
    assertTrue(type instanceof RowType);
    RowType rowType = (RowType) type;
    assertEquals(2, rowType.getFieldCount());
    assertTrue(rowType.getFields().get(0).getType() instanceof MapType);
    assertTrue(rowType.getFields().get(1).getType() instanceof MapType);
  }

  @Test
  void toLogicalType_repeatedMessageTest_nestedMessage() {
    LogicalType type =
        ProtoToLogicalType.toLogicalType(TestRepeatedNested.RepeatedMessageTest.getDescriptor());
    assertNotNull(type);
    assertTrue(type instanceof RowType);
    RowType rowType = (RowType) type;
    assertTrue(rowType.getFieldCount() >= 1);
    LogicalType dField =
        rowType.getFields().stream()
            .filter(f -> "d".equals(f.getName()))
            .findFirst()
            .map(f -> f.getType())
            .orElse(null);
    assertNotNull(dField);
    assertTrue(dField instanceof RowType);
  }
}
