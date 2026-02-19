package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

class LogicalTypeToDDLTest {

  @Test
  void toDDLTypeString_varchar() {
    assertEquals("STRING NOT NULL", LogicalTypeToDDL.toDDLTypeString(new VarCharType(false, 100)));
    assertEquals("STRING", LogicalTypeToDDL.toDDLTypeString(new VarCharType(true, 50)));
  }

  @Test
  void toDDLTypeString_intAndBigint() {
    assertEquals("INT NOT NULL", LogicalTypeToDDL.toDDLTypeString(new IntType(false)));
    assertEquals("BIGINT", LogicalTypeToDDL.toDDLTypeString(new BigIntType(true)));
  }

  @Test
  void toDDLTypeString_double() {
    assertEquals("DOUBLE NOT NULL", LogicalTypeToDDL.toDDLTypeString(new DoubleType(false)));
  }

  @Test
  void toDDLTypeString_decimal() {
    LogicalType t = new DecimalType(true, 10, 2);
    assertEquals("DECIMAL(10,2)", LogicalTypeToDDL.toDDLTypeString(t));
  }

  @Test
  void toDDLTypeString_dateAndTime() {
    assertEquals("DATE", LogicalTypeToDDL.toDDLTypeString(new DateType(true)));
    assertEquals("TIME(3)", LogicalTypeToDDL.toDDLTypeString(new TimeType(true, 3)));
    assertEquals(
        "TIMESTAMP_LTZ(9)", LogicalTypeToDDL.toDDLTypeString(new LocalZonedTimestampType(true, 9)));
  }

  @Test
  void toDDLTypeString_row() {
    RowType rowType =
        new RowType(
            false,
            List.of(
                new RowField("a", new IntType(false)),
                new RowField("b", new VarCharType(true, 10))));
    assertEquals(
        "ROW<`a` INT NOT NULL, `b` STRING> NOT NULL", LogicalTypeToDDL.toDDLTypeString(rowType));
  }

  @Test
  void toDDLTypeString_arrayAndMap() {
    ArrayType arrayType = new ArrayType(false, new IntType(false));
    assertEquals("ARRAY<INT NOT NULL> NOT NULL", LogicalTypeToDDL.toDDLTypeString(arrayType));
    MapType mapType = new MapType(false, new VarCharType(false, 10), new IntType(false));
    assertEquals(
        "MAP<STRING NOT NULL, INT NOT NULL> NOT NULL", LogicalTypeToDDL.toDDLTypeString(mapType));
  }

  @Test
  void toTableSchemaDDL() {
    RowType rowType =
        new RowType(
            false,
            List.of(
                new RowField("id", new BigIntType(false)),
                new RowField("name", new VarCharType(true, 255))));
    String ddl = LogicalTypeToDDL.toTableSchemaDDL(rowType);
    assertEquals("`id` BIGINT NOT NULL, `name` STRING", ddl);
  }

  @Test
  void toTableSchemaDDL_throwsForNonRowType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> LogicalTypeToDDL.toTableSchemaDDL(new IntType(false)));
    assertTrue(ex.getMessage().startsWith("Root type must be RowType"));
  }

  @Test
  void toStrictNotNull_scalar() {
    LogicalType nullable = new IntType(true);
    LogicalType result = LogicalTypeToDDL.toStrictNotNull(nullable);
    assertFalse(result.isNullable());
  }

  @Test
  void toStrictNotNull_preservesTimestampDateTimeNullable() {
    assertTrue(LogicalTypeToDDL.toStrictNotNull(new LocalZonedTimestampType(true, 9)).isNullable());
    assertTrue(LogicalTypeToDDL.toStrictNotNull(new DateType(true)).isNullable());
    assertTrue(LogicalTypeToDDL.toStrictNotNull(new TimeType(true, 3)).isNullable());
  }

  @Test
  void toStrictNotNull_withKeepNullablePaths() {
    RowType rowType =
        new RowType(
            true,
            List.of(
                new RowField("a", new IntType(true)),
                new RowField("b", new VarCharType(true, 10))));
    LogicalType result = LogicalTypeToDDL.toStrictNotNull(rowType, "", Set.of("b"));
    RowType out = (RowType) result;
    assertEquals(2, out.getFieldCount());
    assertFalse(out.getFields().get(0).getType().isNullable());
    assertTrue(out.getFields().get(1).getType().isNullable());
  }

  @Test
  void toStrictNotNull_arrayElementPath() {
    LogicalType elementType = new RowType(true, List.of(new RowField("x", new IntType(true))));
    ArrayType arrayType = new ArrayType(false, elementType);
    LogicalType result = LogicalTypeToDDL.toStrictNotNull(arrayType, "arr", Set.of("arr[]"));
    ArrayType out = (ArrayType) result;
    assertFalse(out.getElementType().isNullable());
  }
}
