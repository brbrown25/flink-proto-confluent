package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.SUBJECT;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.orderSchema;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.schemaIdOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.metrics.SchemaRegistryMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Covers GitHub issue #175 on the sink: {@code numSerializeErrors} and the Schema Registry metrics,
 * with the real registry client against a local HTTP server, so that what the metrics report can be
 * compared with the requests that actually reached the "registry".
 */
class ProtoRowDataSerializationSchemaMetricTest {

  private static final RowType ROW_TYPE = rowType("content", "date_time");
  private static final String LATEST_PATH = "/subjects/" + SUBJECT + "/versions/latest";
  private static final String JSON = "application/vnd.schemaregistry.v1+json";
  private static final int SCHEMA_ID = 7;

  private HttpServer server;
  private final List<String> requests = new CopyOnWriteArrayList<>();
  private volatile int lookupStatus = 200;
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

  /**
   * A registry whose subject holds one version of the Order schema: the latest-version lookup
   * answers with {@link #lookupStatus}, everything else (the serializer's lookup of the schema's
   * ID) with the ID.
   */
  private void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    String path = exchange.getRequestURI().getPath();
    requests.add(method + " " + path);
    exchange.getRequestBody().readAllBytes();
    boolean failing = LATEST_PATH.equals(path) && lookupStatus != 200;
    int status = failing ? lookupStatus : 200;
    String body =
        failing
            ? "{\"error_code\":50001,\"message\":\"Error in the backend\"}"
            : new ObjectMapper()
                .writeValueAsString(
                    Map.of(
                        "subject",
                        SUBJECT,
                        "version",
                        1,
                        "id",
                        SCHEMA_ID,
                        "schemaType",
                        "PROTOBUF",
                        "schema",
                        orderSchema().canonicalString()));
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", JSON);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private ProtoConfluentFormatConfig config() {
    Configuration options = new Configuration();
    options.set(
        ProtoConfluentFormatOptions.URL, "http://127.0.0.1:" + server.getAddress().getPort());
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, false);
    // The client retries 5xx answers (three more attempts by default) before it gives up; keep
    // the retries, drop the one-second waits between them.
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of("retries.wait.ms", "1", "retries.max.wait.ms", "5"));
    return new ProtoConfluentFormatConfig(options);
  }

  private SerializationSchema.InitializationContext context() {
    return new SerializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return listener.getMetricGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(
            ProtoRowDataSerializationSchemaMetricTest.class.getClassLoader());
      }
    };
  }

  private long counter(String name) {
    return listener.getCounter(name).orElseThrow().getCount();
  }

  private long latencyCount() {
    return listener.getHistogram(SchemaRegistryMetrics.LATENCY_MS).orElseThrow().getCount();
  }

  @Test
  void open_registersTheMetricsAtZero() {
    new ProtoRowDataSerializationSchema(ROW_TYPE, config()).open(context());

    assertEquals(0L, counter(ProtoRowDataSerializationSchema.NUM_SERIALIZE_ERRORS));
    assertEquals(0L, counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(0L, counter(SchemaRegistryMetrics.ERRORS));
    assertEquals(0L, latencyCount());
    assertTrue(requests.isEmpty(), "opening the sink sends nothing to the registry");
  }

  @Test
  void serialize_countsRegistryRequests_notTheRowsServedFromTheCaches() {
    ProtoRowDataSerializationSchema schema = new ProtoRowDataSerializationSchema(ROW_TYPE, config());
    schema.open(context());

    assertEquals(SCHEMA_ID, schemaIdOf(schema.serialize(row("a", "b"))));

    assertEquals("GET " + LATEST_PATH, requests.get(0));
    long requestsForFirstRow = counter(SchemaRegistryMetrics.REQUESTS);
    assertEquals(requests.size(), requestsForFirstRow);
    assertEquals(0L, counter(SchemaRegistryMetrics.ERRORS));
    assertEquals(requestsForFirstRow, latencyCount());

    // More rows: the sink's schema cache and the client's caches answer, the registry is not
    // asked, and nothing is counted.
    schema.serialize(row("c", "d"));
    schema.serialize(row("e", "f"));

    assertEquals(requestsForFirstRow, requests.size());
    assertEquals(requestsForFirstRow, counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(requestsForFirstRow, latencyCount());
    assertEquals(0L, counter(ProtoRowDataSerializationSchema.NUM_SERIALIZE_ERRORS));
  }

  @Test
  void serialize_whenRegistryFails_countsOneFailedRequestHoweverOftenTheClientRetried() {
    lookupStatus = 500;
    ProtoRowDataSerializationSchema schema = new ProtoRowDataSerializationSchema(ROW_TYPE, config());
    schema.open(context());

    assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));

    assertTrue(requests.size() > 1, "the client retries a 500: " + requests);
    assertEquals(1L, counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(1L, counter(SchemaRegistryMetrics.ERRORS));
    assertEquals(1L, latencyCount());
    assertEquals(1L, counter(ProtoRowDataSerializationSchema.NUM_SERIALIZE_ERRORS));

    // Nothing was cached, so the next row asks again and fails again.
    assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));

    assertEquals(2L, counter(SchemaRegistryMetrics.REQUESTS));
    assertEquals(2L, counter(SchemaRegistryMetrics.ERRORS));
    assertEquals(2L, counter(ProtoRowDataSerializationSchema.NUM_SERIALIZE_ERRORS));
  }

  @Test
  void serialize_countsEveryFailedRow_andNoSuccessfulOne_withStubbedRegistry() throws Exception {
    StubSchemaRegistryClient registry = new StubSchemaRegistryClient();
    registry.seed(SUBJECT, orderSchema());
    registry.failLookupsWith(new RestClientException("Unauthorized", 401, 401));
    ProtoRowDataSerializationSchema schema =
        new ProtoRowDataSerializationSchema(ROW_TYPE, config()) {
          private static final long serialVersionUID = 1L;

          @Override
          SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
            return registry;
          }
        };
    schema.open(context());

    assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));
    assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));
    assertEquals(2L, counter(ProtoRowDataSerializationSchema.NUM_SERIALIZE_ERRORS));

    registry.failLookupsWith(null);
    schema.serialize(row("a", "b"));

    assertEquals(2L, counter(ProtoRowDataSerializationSchema.NUM_SERIALIZE_ERRORS));
  }

  @Test
  void serialize_withoutMetricGroup_failsTheSameWayAndRegistersNothing() {
    lookupStatus = 500;
    ProtoRowDataSerializationSchema schema = new ProtoRowDataSerializationSchema(ROW_TYPE, config());
    schema.open(null);

    assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));

    assertTrue(
        listener.getCounter(ProtoRowDataSerializationSchema.NUM_SERIALIZE_ERRORS).isEmpty());
    assertTrue(listener.getCounter(SchemaRegistryMetrics.REQUESTS).isEmpty());
  }
}
