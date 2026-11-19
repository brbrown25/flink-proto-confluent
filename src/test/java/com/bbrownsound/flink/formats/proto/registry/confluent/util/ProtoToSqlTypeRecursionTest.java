package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.junit.jupiter.api.Test;

/**
 * Derived types for recursive messages (issue #216): each {@link RecursionStrategy} maps the field
 * that closes the cycle, wherever it is (a plain, repeated, map or oneof field), and the Flink-free
 * and Flink renderings agree.
 */
class ProtoToSqlTypeRecursionTest {

  static final Descriptor TREE =
      descriptor(
          "syntax = \"proto3\"; package rec; message Tree { string name = 1; Tree child = 2;"
              + " repeated Tree children = 3; map<string, Tree> by_name = 4;"
              + " oneof kind { string label = 5; Tree alt = 6; } }");

  static Descriptor descriptor(String schema) {
    return new ProtobufSchema(schema).toDescriptor();
  }

  /** Renders the columns with both paths, checking they agree. */
  static String columns(Descriptor descriptor, RecursionStrategy recursion) {
    String ddl = String.join(", ", ProtoToSqlType.toSqlType(descriptor, recursion).toColumnDefinitions());
    assertEquals(
        ddl,
        LogicalTypeToDDL.toTableSchemaDDL(ProtoToLogicalType.toLogicalType(descriptor, recursion)));
    return ddl;
  }

  @Test
  void failIsTheDefault() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> ProtoToSqlType.toSqlType(TREE));
    assertTrue(e.getMessage().startsWith("Cyclic schemas are not supported."), e.getMessage());
    assertTrue(e.getMessage().contains("'rec.Tree' contains itself"), e.getMessage());
    assertTrue(e.getMessage().contains("'recursive-message' option"), e.getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> ProtoToSqlType.toSqlType(TREE, RecursionStrategy.FAIL));
  }

  @Test
  void bytesMapsEveryRecursiveFieldToBytes() {
    assertEquals(
        "`name` STRING, `child` VARBINARY, `children` ARRAY<VARBINARY> NOT NULL,"
            + " `by_name` MAP<STRING, VARBINARY> NOT NULL, `kind` ROW<`label` STRING, `alt` VARBINARY>",
        columns(TREE, RecursionStrategy.BYTES));
  }

  @Test
  void jsonMapsEveryRecursiveFieldToString() {
    assertEquals(
        "`name` STRING, `child` STRING, `children` ARRAY<STRING> NOT NULL,"
            + " `by_name` MAP<STRING, STRING> NOT NULL, `kind` ROW<`label` STRING, `alt` STRING>",
        columns(TREE, RecursionStrategy.JSON));
  }

  @Test
  void rowUnrollsToTheDepthAndLeavesOutDeeperLevels() {
    Descriptor list = descriptor("syntax = \"proto3\"; message Node { string id = 1; Node next = 2; }");
    assertEquals(
        "`id` STRING, `next` ROW<`id` STRING>", columns(list, RecursionStrategy.row(1)));
    assertEquals(
        "`id` STRING, `next` ROW<`id` STRING, `next` ROW<`id` STRING>>",
        columns(list, RecursionStrategy.row(2)));
  }

  @Test
  void rowLeavesOutRepeatedAndMapFieldsButKeepsOneofCasesAsBytes() {
    String leaf = "ROW<`name` STRING, `kind` ROW<`label` STRING, `alt` VARBINARY>>";
    assertEquals(
        "`name` STRING, `child` "
            + leaf
            + ", `children` ARRAY<"
            + leaf
            + " NOT NULL> NOT NULL, `by_name` MAP<STRING, "
            + leaf
            + "> NOT NULL, `kind` ROW<`label` STRING, `alt` "
            + leaf
            + ">",
        columns(TREE, RecursionStrategy.row(1)));
  }

  @Test
  void mutualRecursionClosesAtTheMessageSeenAgain() {
    Descriptor a =
        descriptor(
            "syntax = \"proto3\"; message A { string x = 1; B b = 2; } message B { A a = 1; }");
    assertEquals("`x` STRING, `b` ROW<`a` VARBINARY>", columns(a, RecursionStrategy.BYTES));
    assertEquals(
        "`x` STRING, `b` ROW<`a` ROW<`x` STRING>>",
        columns(a, RecursionStrategy.row(1)));
  }

  @Test
  void anEmptyMessageStillMapsToAnEmptyRow() {
    Descriptor empty =
        descriptor("syntax = \"proto3\"; message E { string x = 1; Empty e = 2; } message Empty {}");
    assertEquals("`x` STRING, `e` ROW<>", columns(empty, RecursionStrategy.row(1)));
  }

  @Test
  void bytesAndJsonRefuseAMessageWithAValueField() {
    Descriptor wrapperLike =
        descriptor("syntax = \"proto3\"; message V { int32 value = 1; V next = 2; }");
    for (RecursionStrategy recursion : new RecursionStrategy[] {RecursionStrategy.BYTES, RecursionStrategy.JSON}) {
      IllegalArgumentException e =
          assertThrows(
              IllegalArgumentException.class,
              () -> ProtoToSqlType.toSqlType(wrapperLike, recursion));
      assertTrue(e.getMessage().contains("field 'V.next'"), e.getMessage());
      assertTrue(e.getMessage().contains("named 'value'"), e.getMessage());
      assertTrue(e.getMessage().contains("'row:<depth>' instead"), e.getMessage());
    }
    assertEquals(
        "`value` INT, `next` ROW<`value` INT>", columns(wrapperLike, RecursionStrategy.row(1)));
  }

  @Test
  void rowRefusesAOneofCaseThatCannotBeBytes() {
    Descriptor wrapperLike =
        descriptor("syntax = \"proto3\"; message W { int32 value = 1; oneof k { W w = 2; } }");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ProtoToSqlType.toSqlType(wrapperLike, RecursionStrategy.row(1)));
    assertTrue(e.getMessage().contains("cannot be left out"), e.getMessage());
  }

  @Test
  void theValueTypeOfAFieldStillFailsOnACycle() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ProtoToSqlType.toValueSqlType(TREE.findFieldByName("child")));
  }
}
