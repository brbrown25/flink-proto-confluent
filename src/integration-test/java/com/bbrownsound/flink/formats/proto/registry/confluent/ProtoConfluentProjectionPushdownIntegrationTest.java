package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestNestedProjection.NestedProjectionTest;
import com.bbrownsound.flink.formats.proto.test.v1.TestNestedProjection.NestedProjectionTest.Inner;
import com.bbrownsound.flink.formats.proto.test.v1.TestNestedProjection.NestedProjectionTest.Payload;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Integration test for GitHub issue #173, driven through SQL and the planner: the planner pushes a
 * query's projection into a {@code proto-confluent} source, and the format builds its decoder for
 * the projected columns only.
 *
 * <p>Nested projection (GitHub issue #217) needs a connector that pushes nested projections into
 * its format, which the filesystem connector does not: {@link NestedProjectionTestSourceFactory}
 * stands in for one. Those tests also run the query, against records whose writer schema a stub
 * Schema Registry in this JVM serves, so they need no containers either.
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

  private static final int NESTED_SCHEMA_ID = 7;

  private HttpServer registry;

  @BeforeEach
  void startRegistry() throws IOException {
    registry = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    registry.createContext("/", ProtoConfluentProjectionPushdownIntegrationTest::serveSchema);
    registry.start();
  }

  @AfterEach
  void stopRegistry() {
    registry.stop(0);
  }

  @Test
  void explain_showsTheNestedProjectionPushedIntoTheSource() {
    String table = createNestedTable("", List.of());

    String plan = tableEnv.explainSql("SELECT payload.a, payload.`inner`.x FROM " + table);

    assertTrue(plan.contains("project=[payload_a, payload_inner_x]"), plan);
    assertTrue(plan.contains("fields=[payload_a, payload_inner_x]"), plan);
  }

  @Test
  void explain_filesystemConnectorPushesTheWholeTopLevelColumn() {
    // The filesystem connector does not push nested projections, so the format gets column f.
    String table = createWideTable("");

    String plan = tableEnv.explainSql("SELECT f.x FROM " + table);

    assertTrue(plan.contains("project=[f]"), plan);
  }

  @Test
  void nestedProjection_readsOnlyTheProjectedFields() throws Exception {
    // payload.f7 is a string field declared TIMESTAMP(3), which the format cannot convert: the
    // query succeeds only because the siblings of the fields it reads are never converted.
    NestedProjectionTest full =
        NestedProjectionTest.newBuilder()
            .setId("order-1")
            .setPayload(
                Payload.newBuilder()
                    .setA(42L)
                    .setInner(Inner.newBuilder().setX("inner-x"))
                    .setText("some text")
                    .setF7("not a timestamp"))
            .setCode(5)
            .build();
    NestedProjectionTest bare = NestedProjectionTest.newBuilder().setId("order-2").build();
    String table = createNestedTable("", List.of(record(full), record(bare)));

    List<Row> rows =
        collect(
            "SELECT id, payload.a, payload.`inner`.x, payload.kind.text, choice.code FROM "
                + table);

    assertEquals(2, rows.size(), rows.toString());
    assertEquals(Row.of("order-1", 42L, "inner-x", "some text", 5), rows.get(0));
    // No payload and no oneof case: the fields under them are NULL.
    assertEquals(Row.of("order-2", null, null, null, null), rows.get(1));

    String plan = tableEnv.explainSql("SELECT payload.f7 FROM " + table);
    assertTrue(plan.contains("project=[payload_f7]"), plan);
    Exception e = assertThrows(Exception.class, () -> collect("SELECT payload.f7 FROM " + table));
    assertTrue(causeMessages(e).contains("TIMESTAMP_WITHOUT_TIME_ZONE"), causeMessages(e));
  }

  @Test
  void nestedProjection_messageClassChecksTheProjectedFieldsOnly() {
    // 'payload.f7' does not match NestedProjectionTest (a string read as TIMESTAMP), so
    // 'message-class' rejects a decoder that reads it; one that reads payload.a is accepted.
    String table =
        createNestedTable(
            "  'proto-confluent.message-class' = '" + NestedProjectionTest.class.getName() + "',",
            List.of());

    String plan = tableEnv.explainSql("SELECT payload.a FROM " + table);
    assertTrue(plan.contains("project=[payload_a]"), plan);

    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> tableEnv.explainSql("SELECT payload.a, payload.f7 FROM " + table));
    assertTrue(causeMessages(e).contains("column 'payload.f7' is TIMESTAMP(3)"), causeMessages(e));
    assertFalse(causeMessages(e).contains("column 'payload.a'"), causeMessages(e));
  }

  private List<Row> collect(String query) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> it = tableEnv.executeSql(query).collect()) {
      it.forEachRemaining(rows::add);
    }
    return rows;
  }

  private String createNestedTable(String extraOptionLines, List<byte[]> records) {
    String table = "nested_projection_src_" + TABLE_COUNTER.incrementAndGet();
    NestedProjectionTestSourceFactory.register(table, records);
    String url = "http://127.0.0.1:" + registry.getAddress().getPort();
    tableEnv.executeSql(
        "CREATE TABLE "
            + table
            + " ("
            + "  id STRING,"
            + "  payload ROW<a BIGINT, b STRING, `inner` ROW<x STRING, y INT>,"
            + "    kind ROW<text STRING, nested ROW<x STRING, y INT>>, f7 TIMESTAMP(3)>,"
            + "  choice ROW<code INT, detail ROW<x STRING, y INT>>"
            + ") WITH ("
            + "  'connector' = '"
            + NestedProjectionTestSourceFactory.IDENTIFIER
            + "',"
            + "  'records' = '"
            + table
            + "',"
            + "  'format' = 'proto-confluent',"
            + "  'proto-confluent.url' = '"
            + url
            + "',"
            + extraOptionLines
            + "  'proto-confluent.subject' = 'nested-value'"
            + ")");
    return table;
  }

  /** Confluent wire format: magic byte, schema ID, message index (first message), payload. */
  private static byte[] record(Message message) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(0);
    out.writeInt(NESTED_SCHEMA_ID);
    out.writeByte(0);
    out.write(message.toByteArray());
    return bytes.toByteArray();
  }

  /** The stub registry: answers the schema-by-ID lookup for NestedProjectionTest. */
  private static void serveSchema(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    boolean known = exchange.getRequestURI().getPath().equals("/schemas/ids/" + NESTED_SCHEMA_ID);
    String body =
        known
            ? new ObjectMapper()
                .writeValueAsString(
                    Map.of(
                        "schemaType",
                        "PROTOBUF",
                        "schema",
                        new ProtobufSchema(NestedProjectionTest.getDescriptor()).canonicalString()))
            : "{\"error_code\":40403,\"message\":\"Schema not found\"}";
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
    exchange.sendResponseHeaders(known ? 200 : 404, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

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
