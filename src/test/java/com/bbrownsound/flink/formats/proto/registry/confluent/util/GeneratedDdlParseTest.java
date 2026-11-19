package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.Descriptors.Descriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The DDL generated for a schema means, to Flink, exactly the type the format derives (issue #222):
 * each column of the generated column list, parsed back with Flink's {@link LogicalTypeParser},
 * equals the derived column type. A bare {@code VARBINARY}, for example, parses as {@code
 * VARBINARY(1)} rather than the derived {@code BYTES}.
 */
class GeneratedDdlParseTest {

  static Stream<Arguments> schemas() {
    List<Arguments> arguments = new ArrayList<>();
    ProtoSqlTypeTest.descriptors()
        .forEach(d -> arguments.add(Arguments.of(d.getFullName(), d, RecursionStrategy.FAIL)));
    Descriptor tree = ProtoToSqlTypeRecursionTest.TREE;
    for (RecursionStrategy recursion :
        List.of(
            RecursionStrategy.BYTES,
            RecursionStrategy.JSON,
            RecursionStrategy.row(1),
            RecursionStrategy.row(2))) {
      arguments.add(Arguments.of(tree.getFullName() + " " + recursion, tree, recursion));
    }
    return arguments.stream();
  }

  private static LogicalType parse(String ddlType) {
    return LogicalTypeParser.parse(ddlType, GeneratedDdlParseTest.class.getClassLoader());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("schemas")
  void generatedColumnsParseToTheDerivedTypes(
      String name, Descriptor descriptor, RecursionStrategy recursion) {
    LogicalType derived = ProtoToLogicalType.toLogicalType(descriptor, recursion);
    // The top-level row is NOT NULL; a ROW<...> written around the column list parses nullable.
    LogicalType expected = derived.copy(true);

    String flinkColumns =
        ProtoToTableDDL.tableSchemaFromDescriptor(descriptor, false, null, recursion);
    assertEquals(expected, parse("ROW<" + flinkColumns + ">"), flinkColumns);

    String cliColumns =
        String.join(", ", ProtoToSqlType.toSqlType(descriptor, recursion).toColumnDefinitions());
    assertEquals(expected, parse("ROW<" + cliColumns + ">"), cliColumns);

    String strict =
        ProtoToTableDDL.tableSchemaFromDescriptor(descriptor, true, null, recursion);
    assertEquals(
        LogicalTypeToDDL.toStrictNotNull(derived, "", null).copy(true),
        parse("ROW<" + strict + ">"),
        strict);
  }
}
