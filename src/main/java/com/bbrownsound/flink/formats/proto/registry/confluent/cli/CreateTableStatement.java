package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoSqlType;
import java.util.List;
import java.util.Map;

/** Renders column lists and {@code CREATE TABLE} statements as Flink SQL text. */
final class CreateTableStatement {

  private static final String INDENT = "  ";

  private CreateTableStatement() {}

  /**
   * Returns the column definitions of a row type, one per line, separated by commas: the part that
   * goes between the parentheses of a {@code CREATE TABLE}.
   */
  static String columns(ProtoSqlType rowType) {
    return String.join(",\n", rowType.toColumnDefinitions());
  }

  /**
   * Returns a complete {@code CREATE TABLE} statement, terminated by a semicolon.
   *
   * @param tableName the table name, quoted as a single identifier
   * @param rowType the table row type
   * @param options the {@code WITH} options in order
   * @return the statement
   */
  static String render(String tableName, ProtoSqlType rowType, Map<String, String> options) {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE ").append(ProtoSqlType.quoteIdentifier(tableName)).append(" (\n");
    List<String> columns = rowType.toColumnDefinitions();
    for (int i = 0; i < columns.size(); i++) {
      sb.append(INDENT).append(columns.get(i)).append(i < columns.size() - 1 ? ",\n" : "\n");
    }
    sb.append(") WITH (\n");
    int i = 0;
    for (Map.Entry<String, String> option : options.entrySet()) {
      sb.append(INDENT)
          .append(quoteLiteral(option.getKey()))
          .append(" = ")
          .append(quoteLiteral(option.getValue()))
          .append(++i < options.size() ? ",\n" : "\n");
    }
    sb.append(");");
    return sb.toString();
  }

  /** Quotes a Flink SQL string literal, doubling any single quote inside it. */
  static String quoteLiteral(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
