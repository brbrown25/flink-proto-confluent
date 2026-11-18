package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoSqlType.Kind;
import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestEnums;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestOneof;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeatedNested;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.bbrownsound.flink.formats.proto.test.v1.TestWrapperTypes;
import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The Flink-free rendering ({@link ProtoSqlType}) must produce exactly what the Flink path ({@link
 * ProtoToLogicalType} + {@link LogicalTypeToDDL}) produces, because the {@code ddl} tool uses the
 * former and the format and {@link ProtoToTableDDL} use the latter.
 */
class ProtoSqlTypeTest {

  /** Every special-cased type: decimal, date, time, timestamp, int8/int16, nested and maps. */
  static final String TYPED_SCHEMA =
      String.join(
          "\n",
          "syntax = \"proto3\";",
          "package ddl.test;",
          "import \"confluent/meta.proto\";",
          "import \"confluent/type/decimal.proto\";",
          "import \"google/type/date.proto\";",
          "import \"google/type/timeofday.proto\";",
          "import \"google/protobuf/timestamp.proto\";",
          "message Typed {",
          "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = { params: [",
          "    { key: \"precision\", value: \"12\" }, { key: \"scale\", value: \"3\" } ] }];",
          "  confluent.type.Decimal plain_amount = 2;",
          "  google.type.Date day = 3;",
          "  google.type.TimeOfDay at = 4;",
          "  google.protobuf.Timestamp ts = 5;",
          "  int32 tiny = 6 [(confluent.field_meta) = { params: [",
          "    { key: \"connect.type\", value: \"int8\" } ] }];",
          "  int32 small = 7 [(confluent.field_meta) = { params: [",
          "    { key: \"connect.type\", value: \"int16\" } ] }];",
          "  repeated Inner inners = 8;",
          "  map<string, Inner> by_key = 9;",
          "  bytes raw = 10;",
          "  confluent.type.Decimal odd = 11 [(confluent.field_meta) = { params: [",
          "    { key: \"precision\", value: \"x\" } ] }];",
          "  message Inner { string note = 1; optional int64 count = 2; }",
          "}");

  static Descriptor typed() {
    return new ProtobufSchema(TYPED_SCHEMA).toDescriptor("ddl.test.Typed");
  }

  static Stream<Descriptor> descriptors() {
    return Stream.of(
        TestSimple.SimpleMessage.getDescriptor(),
        TestBig.BigPbMessage.getDescriptor(),
        TestWrapperTypes.WrapperTypesTest.getDescriptor(),
        TestRepeated.RepeatedTest.getDescriptor(),
        TestRepeatedNested.RepeatedMessageTest.getDescriptor(),
        TestMap.MapTest.getDescriptor(),
        TestOneof.MessageWithOneof.getDescriptor(),
        TestEnums.TestEnum.getDescriptor(),
        typed());
  }

  @ParameterizedTest
  @MethodSource("descriptors")
  void columnsMatchTheFlinkRendering(Descriptor descriptor) {
    assertEquals(
        ProtoToTableDDL.tableSchemaFromDescriptor(descriptor),
        String.join(", ", ProtoToSqlType.toSqlType(descriptor).toColumnDefinitions()));
  }

  @ParameterizedTest
  @MethodSource("descriptors")
  void strictNotNullMatchesTheFlinkRendering(Descriptor descriptor) {
    assertEquals(
        ProtoToTableDDL.tableSchemaFromDescriptor(descriptor, true),
        String.join(
            ", ", ProtoToSqlType.toSqlType(descriptor).toStrictNotNull(null).toColumnDefinitions()));
  }

  @Test
  void keepNullablePathsMatchTheFlinkRendering() {
    Set<String> keep = Set.of("raw", "inners[].note", "by_key.value.count", "by_key.key");
    assertEquals(
        ProtoToTableDDL.tableSchemaFromDescriptor(typed(), true, keep),
        String.join(", ", ProtoToSqlType.toSqlType(typed()).toStrictNotNull(keep).toColumnDefinitions()));
  }

  @ParameterizedTest
  @MethodSource("descriptors")
  void convertsToTheSameLogicalTypeAsBefore(Descriptor descriptor) {
    LogicalType logicalType = ProtoToLogicalType.toLogicalType(descriptor);
    assertEquals(
        LogicalTypeToDDL.toTableSchemaDDL(logicalType),
        String.join(", ", ProtoToSqlType.toSqlType(descriptor).toColumnDefinitions()));
    assertFalse(logicalType.isNullable(), "the top-level row is NOT NULL");
  }

  @Test
  void rendersSpecialTypes() {
    List<String> columns = ProtoToSqlType.toSqlType(typed()).toColumnDefinitions();
    assertEquals("`amount` DECIMAL(12,3)", columns.get(0));
    assertEquals("`plain_amount` DECIMAL(10,0)", columns.get(1));
    assertEquals("`day` DATE", columns.get(2));
    assertEquals("`at` TIME(3)", columns.get(3));
    assertEquals("`ts` TIMESTAMP_LTZ(9)", columns.get(4));
    assertEquals("`tiny` TINYINT", columns.get(5));
    assertEquals("`small` SMALLINT", columns.get(6));
    assertEquals(
        "`inners` ARRAY<ROW<`note` STRING, `count` BIGINT> NOT NULL> NOT NULL", columns.get(7));
    assertEquals(
        "`by_key` MAP<STRING, ROW<`note` STRING, `count` BIGINT>> NOT NULL", columns.get(8));
    assertEquals("`raw` VARBINARY", columns.get(9));
    assertEquals("`odd` DECIMAL(10,0)", columns.get(10), "an unparsable precision is ignored");
  }

  @Test
  void decimalDefaultsMatchFlink() {
    assertEquals(DecimalType.DEFAULT_PRECISION, ProtoToSqlType.DEFAULT_DECIMAL_PRECISION);
    assertEquals(DecimalType.DEFAULT_SCALE, ProtoToSqlType.DEFAULT_DECIMAL_SCALE);
  }

  @Test
  void cyclicSchemasAreRejected() {
    Descriptor cyclic =
        new ProtobufSchema("syntax = \"proto3\"; message Node { string id = 1; Node next = 2; }")
            .toDescriptor();
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> ProtoToSqlType.toSqlType(cyclic));
    assertEquals("Cyclic schemas are not supported.", e.getMessage());
  }

  @Test
  void accessorsAndFactories() {
    ProtoSqlType string = ProtoSqlType.of(Kind.STRING, true);
    assertEquals(Kind.STRING, string.getKind());
    assertTrue(string.isNullable());
    assertEquals("STRING NOT NULL", string.withNullable(false).toString());
    assertTrue(string.getFields().isEmpty());
    assertNull(string.getElementType());
    assertNull(string.getKeyType());
    assertNull(string.getValueType());
    ProtoSqlType decimal = ProtoSqlType.decimal(false, 5, 2);
    assertEquals(5, decimal.getPrecision());
    assertEquals(2, decimal.getScale());
    for (Kind kind :
        List.of(Kind.DECIMAL, Kind.TIME, Kind.TIMESTAMP_LTZ, Kind.ROW, Kind.ARRAY, Kind.MAP)) {
      assertThrows(IllegalArgumentException.class, () -> ProtoSqlType.of(kind, true));
    }
    assertThrows(IllegalStateException.class, string::toColumnDefinitions);
  }

  @Test
  void quotesIdentifiers() {
    assertEquals("`a``b`", ProtoSqlType.quoteIdentifier("a`b"));
    assertEquals("`orders.v1`", ProtoSqlType.quoteIdentifier("orders.v1"));
  }
}
