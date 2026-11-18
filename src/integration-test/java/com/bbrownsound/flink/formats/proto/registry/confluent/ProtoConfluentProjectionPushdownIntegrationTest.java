package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration test for GitHub issue #173, driven through SQL and the planner: the planner pushes a
 * query's projection into a {@code proto-confluent} source, and the format builds its decoder for
 * the projected columns only.
 *
 * <p>The filesystem connector stands in for a source that pushes projections down into its format
 * (the Kafka connector does not push projections into its sources). Everything here happens while
 * the query is planned, before anything reads a file or connects to the Schema Registry, so this
 * test needs no containers.
 */
@DisplayName("Proto-confluent projection pushdown through SQL")
class ProtoConfluentProjectionPushdownIntegrationTest {

  private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

  /**
   * Streaming mode: in batch mode the planner also builds the source's runtime provider for the
   * whole table while it validates the source, before any projection is pushed down.
   */
  private final TableEnvironment tableEnv =
      TableEnvironment.create(EnvironmentSettings.inStreamingMode());

  @Test
  void explain_showsTheProjectionPushedIntoTheSource() {
    String table = createWideTable("");

    String plan = tableEnv.explainSql("SELECT a, b FROM " + table);

    assertTrue(plan.contains("project=[a, b]"), plan);
    assertTrue(plan.contains("fields=[a, b]"), plan);
  }

  @Test
  void decoderIsBuiltForTheProjectedColumnsOnly() {
    // 'not_in_message' has no field in SimpleMessage, so 'message-class' rejects a decoder that
    // includes it. A query that does not read the column plans; one that reads it does not.
    String table =
        createSourceTable(
            "  `content` STRING, `date_time` STRING, `not_in_message` STRING",
            "  'proto-confluent.message-class' = '"
                + TestSimple.SimpleMessage.class.getName()
                + "',");

    assertNotNull(tableEnv.explainSql("SELECT content FROM " + table));

    ValidationException e =
        assertThrows(
            ValidationException.class, () -> tableEnv.explainSql("SELECT * FROM " + table));
    assertTrue(causeMessages(e).contains("column 'not_in_message' has no field"), causeMessages(e));
  }

  private String createWideTable(String extraOptionLines) {
    return createSourceTable(
        "  a STRING, b BIGINT, c DOUBLE, d ARRAY<STRING>, e MAP<STRING, STRING>,"
            + "  f ROW<x STRING, y INT>",
        extraOptionLines);
  }

  private String createSourceTable(String columns, String extraOptionLines) {
    String table = "projection_src_" + TABLE_COUNTER.incrementAndGet();
    tableEnv.executeSql(
        "CREATE TABLE "
            + table
            + " ("
            + columns
            + ") WITH ("
            + "  'connector' = 'filesystem',"
            + "  'path' = 'file:///tmp/proto-confluent-projection-unused',"
            + "  'format' = 'proto-confluent',"
            + "  'proto-confluent.url' = 'http://localhost:8081',"
            + extraOptionLines
            + "  'proto-confluent.subject' = 'wide-value'"
            + ")");
    return table;
  }

  private static String causeMessages(Throwable error) {
    StringBuilder out = new StringBuilder();
    for (Throwable t = error; t != null; t = t.getCause()) {
      out.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" | ");
    }
    return out.toString();
  }
}
