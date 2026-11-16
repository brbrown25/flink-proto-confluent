package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.metrics.SchemaRegistryMetrics;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers GitHub issue #175 on the source: the Schema Registry metrics and the {@code
 * numCachedSchemas} gauge, with the real registry client against a local HTTP server, so that what
 * the metrics report can be compared with the requests that actually reached the "registry".
 */
class ProtoRowDataDeserializationSchemaRegistryMetricTest {

  private static final int KNOWN_ID = 7;
  private static final int OTHER_KNOWN_ID = 8;
  private static final int UNKNOWN_ID = 99;
  private static final String JSON = "application/vnd.schemaregistry.v1+json";
  private static final RowType ROW_TYPE =
      (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());

  private HttpServer server;
  private final List<String> requests = new CopyOnWriteArrayList<>();
  private MetricListener listener;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", this::handle);
    server.start();
    listener = new MetricListener();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  /** Answers a schema-by-ID lookup of a known ID with the SimpleMessage schema, else a 404. */
  private void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    requests.add(exchange.getRequestMethod() + " " + path);
    exchange.getRequestBody().readAllBytes();
    boolean known =
        path.equals("/schemas/ids/" + KNOWN_ID) || path.equals("/schemas/ids/" + OTHER_KNOWN_ID);
    String body =
        known
            ? new ObjectMapper()
                .writeValueAsString(
                    Map.of(
                        "schemaType",
                        "PROTOBUF",
                        "schema",
                        new ProtobufSchema(TestSimple.SimpleMessage.getDescriptor())
                            .canonicalString()))
            : "{\"error_code\":40403,\"message\":\"Schema not found\"}";
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", JSON);
    exchange.sendResponseHeaders(known ? 200 : 404, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private ProtoRowDataDeserializationSchema openSource(boolean withMetricGroup) {
    String url = "http://127.0.0.1:" + server.getAddress().getPort();
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            url, "test-topic", false, Map.of("schema.registry.url", url));
    config.onDeserializeError = OnDeserializeError.SKIP;
    var schema = new ProtoRowDataDeserializationSchema(ROW_TYPE, null, config);
    schema.open(withMetricGroup ? context() : null);
    return schema;
  }

  private DeserializationSchema.InitializationContext context() {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return listener.getMetricGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(
            ProtoRowDataDeserializationSchemaRegistryMetricTest.class.getClassLoader());
      }
    };
  }

  /** Confluent wire format: magic byte, schema ID, message index (first message), payload. */
  private static byte[] wireBytes(int schemaId) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(0);
    out.writeInt(schemaId);
    out.writeByte(0);
    out.write(
        TestSimple.SimpleMessage.newBuilder()
            .setContent("hello")
            .setDateTime("2026-01-01")
            .build()
            .toByteArray());
    return bytes.toByteArray();
  }

  private long counter(String name) {
    return listener.getCounter(name).orElseThrow().getCount();
  }

  private long latencyCount() {
    return listener.getHistogram(SchemaRegistryMetrics.LATENCY_MS).orElseThrow().getCount();
  }

  @SuppressWarnings("unchecked")
  private int cachedSchemas() {
    Gauge<Integer> gauge =
        (Gauge<Integer>)
            (Gauge<?>)
                listener
                    .getGauge(ProtoRowDataDeserializationSchema.NUM_CACHED_SCHEMAS)
                    .orElseThrow();
    return gauge.getValue();
  }

  @Test
  void open_registersTheMetricsAtZero() {
    openSource(true);

    assertEquals(0L, counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(0L, counter(SchemaRegistryMetrics.ERRORS));
    assertEquals(0L, latencyCount());
    assertEquals(0, cachedSchemas());
    assertTrue(requests.isEmpty(), "opening the source sends nothing to the registry");
  }

  @Test
  void deserialize_countsTheRegistryRequestOnce_notTheCacheHitsAfterIt() throws IOException {
    ProtoRowDataDeserializationSchema schema = openSource(true);

    RowData first = schema.deserialize(wireBytes(KNOWN_ID));

    assertNotNull(first);
    assertEquals("hello", first.getString(0).toString());
    assertEquals(requests.size(), counter(SchemaRegistryMetrics.REQUESTS));
    long requestsForFirstRecord = counter(SchemaRegistryMetrics.REQUESTS);
    assertTrue(requestsForFirstRecord >= 1, "the writer schema is fetched from the registry");
    assertEquals(0L, counter(SchemaRegistryMetrics.ERRORS));
    assertEquals(requestsForFirstRecord, latencyCount());
    assertEquals(1, cachedSchemas());

    // The same writer schema again: the client answers from its cache, nothing reaches the
    // registry, and nothing is counted.
    assertNotNull(schema.deserialize(wireBytes(KNOWN_ID)));
    assertNotNull(schema.deserialize(wireBytes(KNOWN_ID)));

    assertEquals(requestsForFirstRecord, requests.size());
    assertEquals(requestsForFirstRecord, counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(requestsForFirstRecord, latencyCount());
    assertEquals(1, cachedSchemas());
  }

  @Test
  void deserialize_secondWriterSchema_isAnotherRequestAndAnotherCachedSchema() throws IOException {
    ProtoRowDataDeserializationSchema schema = openSource(true);
    schema.deserialize(wireBytes(KNOWN_ID));
    long requestsForFirstSchema = counter(SchemaRegistryMetrics.REQUESTS);

    assertNotNull(schema.deserialize(wireBytes(OTHER_KNOWN_ID)));

    assertTrue(counter(SchemaRegistryMetrics.REQUESTS) > requestsForFirstSchema);
    assertEquals(requests.size(), counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(0L, counter(SchemaRegistryMetrics.ERRORS));
    assertEquals(2, cachedSchemas());
  }

  @Test
  void deserialize_whenRegistryDoesNotKnowTheSchema_countsAnError() throws IOException {
    ProtoRowDataDeserializationSchema schema = openSource(true);

    assertNull(schema.deserialize(wireBytes(UNKNOWN_ID)));

    assertEquals(requests.size(), counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(counter(SchemaRegistryMetrics.REQUESTS), counter(SchemaRegistryMetrics.ERRORS));
    assertTrue(counter(SchemaRegistryMetrics.ERRORS) >= 1);
    assertEquals(counter(SchemaRegistryMetrics.REQUESTS), latencyCount());
    assertEquals(1L, counter("numDeserializeErrors"));
    assertEquals(0, cachedSchemas(), "no converter is cached for a schema that was not resolved");
  }

  @Test
  void deserialize_withoutMetricGroup_stillWorksAndRegistersNothing() throws IOException {
    ProtoRowDataDeserializationSchema schema = openSource(false);

    assertNotNull(schema.deserialize(wireBytes(KNOWN_ID)));

    assertEquals(List.of("GET /schemas/ids/" + KNOWN_ID), requests);
    assertTrue(listener.getCounter(SchemaRegistryMetrics.REQUESTS).isEmpty());
    assertTrue(listener.getGauge(ProtoRowDataDeserializationSchema.NUM_CACHED_SCHEMAS).isEmpty());
  }
}
