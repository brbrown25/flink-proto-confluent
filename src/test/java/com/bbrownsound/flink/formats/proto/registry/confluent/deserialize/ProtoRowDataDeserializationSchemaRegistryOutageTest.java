package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
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
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Covers GitHub issue #232: a Schema Registry that cannot be reached, refuses the credentials,
 * throttles or fails with a 5xx says nothing about the record, so the source fails the task
 * whatever {@code on-deserialize-error} says, and nothing is dead-lettered. Records the registry
 * does answer for (an ID it says does not exist) and records that cannot be decoded are still
 * skipped and dead-lettered with {@code skip}.
 *
 * <p>Real components only: the real Confluent registry client against a closed port and against a
 * local HTTP server that answers the way a registry (or a proxy in front of one) does, and a real
 * Kafka broker (Testcontainers) for the dead-letter topic. Dead-letter delivery is {@code sync}, so
 * a dead letter, if one were sent, is acknowledged before {@code deserialize} returns.
 */
@Testcontainers
@DisplayName("ProtoRowDataDeserializationSchema on a Schema Registry outage")
class ProtoRowDataDeserializationSchemaRegistryOutageTest {

  private static final int SCHEMA_ID = 7;
  private static final String SOURCE_TOPIC = "orders";
  private static final String JSON = "application/vnd.schemaregistry.v1+json";
  private static final RowType ROW_TYPE =
      (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());

  @Container
  static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

  private HttpServer server;
  private MetricListener listener;
  private String deadLetterTopic;

  /** What the "registry" answers to a schema-by-ID lookup; {@code 200} serves the schema. */
  private volatile int status = 200;

  private volatile String contentType = JSON;
  private volatile String body;

  @BeforeEach
  void setUp() throws Exception {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", this::handle);
    server.start();
    listener = new MetricListener();
    deadLetterTopic = "dlq-registry-outage-" + UUID.randomUUID();
    try (AdminClient admin =
        AdminClient.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin
          .createTopics(List.of(new NewTopic(deadLetterTopic, 1, (short) 1)))
          .all()
          .get(60, SECONDS);
    }
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  private void handle(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    String path = exchange.getRequestURI().getPath();
    if (status == 200 && path.equals("/schemas/ids/" + SCHEMA_ID)) {
      respond(
          exchange,
          200,
          JSON,
          new ObjectMapper()
              .writeValueAsString(
                  Map.of(
                      "schemaType",
                      "PROTOBUF",
                      "schema",
                      new ProtobufSchema(TestSimple.SimpleMessage.getDescriptor())
                          .canonicalString())));
    } else if (status == 200) {
      respond(exchange, 404, JSON, "{\"error_code\":40403,\"message\":\"Schema not found\"}");
    } else {
      respond(exchange, status, contentType, body);
    }
  }

  private static void respond(HttpExchange exchange, int status, String contentType, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private void answer(int status, String contentType, String body) {
    this.status = status;
    this.contentType = contentType;
    this.body = body;
  }

  private String registryUrl() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private ProtoRowDataDeserializationSchema openSource(
      String registryUrl, OnDeserializeError onDeserializeError) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, registryUrl);
    options.set(ProtoConfluentFormatOptions.TOPIC, SOURCE_TOPIC);
    options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, onDeserializeError);
    // The client retries 5xx and 429 answers before it gives up; keep the retries, drop the
    // one-second waits between them.
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of("retries.wait.ms", "1", "retries.max.wait.ms", "5"));
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, deadLetterTopic);
    options.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
        Map.of("bootstrap.servers", kafka.getBootstrapServers()));
    options.setString("dead-letter.delivery", "sync");
    var schema =
        new ProtoRowDataDeserializationSchema(
            ROW_TYPE, null, new ProtoConfluentFormatConfig(options));
    schema.open(context());
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
            ProtoRowDataDeserializationSchemaRegistryOutageTest.class.getClassLoader());
      }
    };
  }

  /** Confluent wire format: magic byte, schema ID, message index (first message), payload. */
  private static byte[] wireBytes(int schemaId, byte[] payload) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(0);
    out.writeInt(schemaId);
    out.writeByte(0);
    out.write(payload);
    return bytes.toByteArray();
  }

  private static byte[] validRecord(int schemaId) throws IOException {
    return wireBytes(
        schemaId,
        TestSimple.SimpleMessage.newBuilder()
            .setContent("hello")
            .setDateTime("2026-01-01")
            .build()
            .toByteArray());
  }

  private static int closedPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }

  private long counter(String name) {
    return listener.getCounter(name).orElseThrow().getCount();
  }

  /** Asserts that the record failed the task and was neither counted nor dead-lettered. */
  private void assertFailsTheTask(ProtoRowDataDeserializationSchema schema, byte[] record) {
    IOException e = assertThrows(IOException.class, () -> schema.deserialize(record));
    assertInstanceOf(SchemaRegistryUnavailableException.class, e);
    assertTrue(
        e.getMessage().contains("Schema Registry"),
        "the failure names the registry, not the record: " + e.getMessage());
    assertEquals(0L, counter("numDeserializeErrors"), "a registry outage is not a bad record");
    assertEquals(0L, counter("numDeadLetterRecords"));
    assertEquals(0L, counter("numDeadLetterErrors"));
    assertEquals(0L, deadLetterTopicEndOffset(), "nothing dead-lettered");
  }

  /** The dead-letter topic's end offset: how many records were ever written to it. */
  private long deadLetterTopicEndOffset() {
    TopicPartition partition = new TopicPartition(deadLetterTopic, 0);
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps())) {
      return consumer.endOffsets(List.of(partition)).get(partition);
    }
  }

  /** Asserts that the record was skipped, counted, and dead-lettered byte for byte. */
  private void assertSkipped(ProtoRowDataDeserializationSchema schema, byte[] record)
      throws IOException {
    assertNull(schema.deserialize(record));
    assertEquals(1L, counter("numDeserializeErrors"));
    assertEquals(1L, counter("numDeadLetterRecords"));
    List<ConsumerRecord<byte[], byte[]>> deadLetters =
        consume(deadLetterTopic, Duration.ofSeconds(30));
    assertEquals(1, deadLetters.size());
    assertArrayEquals(record, deadLetters.get(0).value());
  }

  @Test
  void skip_registryConnectionRefused_failsTheTaskAndDeadLettersNothing() throws IOException {
    try (var schema = openSource("http://127.0.0.1:" + closedPort(), OnDeserializeError.SKIP)) {
      assertFailsTheTask(schema, validRecord(SCHEMA_ID));
      assertTrue(
          counter(SchemaRegistryMetrics.ERRORS) >= 1,
          "the failed registry request is what the registry error metric counts");
    }
  }

  /** HTTP status, content type and body of answers that say nothing about the record. */
  static Stream<Arguments> registryFailures() {
    return Stream.of(
        Arguments.of(500, JSON, "{\"error_code\":50001,\"message\":\"Error in the backend\"}"),
        Arguments.of(502, "text/html", "<html>Bad Gateway</html>"),
        Arguments.of(503, "text/html", "<html>Service Unavailable</html>"),
        Arguments.of(504, "text/html", "<html>Gateway Timeout</html>"),
        Arguments.of(401, JSON, "{\"error_code\":401,\"message\":\"Unauthorized\"}"),
        Arguments.of(403, JSON, "{\"error_code\":40301,\"message\":\"Forbidden\"}"),
        Arguments.of(408, JSON, "{\"error_code\":408,\"message\":\"Request Timeout\"}"),
        Arguments.of(429, JSON, "{\"error_code\":429,\"message\":\"Too Many Requests\"}"),
        // A 404 that is not the registry saying "schema not found", e.g. a proxy or a wrong URL
        // path: every record would get it, so it is not the record's fault either.
        Arguments.of(404, "text/html", "<html><body>404 Not Found</body></html>"));
  }

  @ParameterizedTest(name = "HTTP {0}")
  @MethodSource("registryFailures")
  void skip_registryFails_failsTheTaskAndDeadLettersNothing(
      int status, String contentType, String body) throws IOException {
    answer(status, contentType, body);
    try (var schema = openSource(registryUrl(), OnDeserializeError.SKIP)) {
      assertFailsTheTask(schema, validRecord(SCHEMA_ID));
    }
  }

  @Test
  void fail_registryFails_failsTheTaskWithoutDeadLetteringTheRecord() throws IOException {
    answer(503, "text/html", "<html>Service Unavailable</html>");
    try (var schema = openSource(registryUrl(), OnDeserializeError.FAIL)) {
      assertFailsTheTask(schema, validRecord(SCHEMA_ID));
    }
  }

  @Test
  void skip_registryRecovers_theSameRecordIsReadAfterTheFailure() throws IOException {
    answer(503, "text/html", "<html>Service Unavailable</html>");
    byte[] record = validRecord(SCHEMA_ID);
    try (var schema = openSource(registryUrl(), OnDeserializeError.SKIP)) {
      assertFailsTheTask(schema, record);

      // What the restarted task does: read the record again, now that the registry is back.
      answer(200, JSON, null);
      RowData row = schema.deserialize(record);

      assertNotNull(row);
      assertEquals("hello", row.getString(0).toString());
      assertEquals(0L, counter("numDeserializeErrors"));
    }
  }

  @Test
  void skip_unknownSchemaId_isStillSkippedAndDeadLettered() throws IOException {
    // The registry is up and says the ID does not exist (404, error code 40403).
    try (var schema = openSource(registryUrl(), OnDeserializeError.SKIP)) {
      assertSkipped(schema, validRecord(99));
    }
  }

  @Test
  void skip_unknownMagicByte_isStillSkippedAndDeadLettered() throws IOException {
    try (var schema = openSource(registryUrl(), OnDeserializeError.SKIP)) {
      assertSkipped(schema, new byte[] {9, 8, 7, 6, 5, 4, 3});
    }
  }

  @Test
  void skip_tooShortForAHeader_isStillSkippedAndDeadLettered() throws IOException {
    try (var schema = openSource(registryUrl(), OnDeserializeError.SKIP)) {
      assertSkipped(schema, new byte[] {0, 1});
    }
  }

  @Test
  void skip_payloadIsNotProtobuf_isStillSkippedAndDeadLettered() throws IOException {
    // The writer schema resolves; the payload after the header is not a valid protobuf message.
    try (var schema = openSource(registryUrl(), OnDeserializeError.SKIP)) {
      assertSkipped(schema, wireBytes(SCHEMA_ID, new byte[] {(byte) 0xFF, (byte) 0xFF, 0x7F}));
    }
  }

  private static Map<String, Object> consumerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "registry-outage-test-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  private static List<ConsumerRecord<byte[], byte[]>> consume(String topic, Duration timeout) {
    List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps())) {
      consumer.subscribe(Collections.singletonList(topic));
      long deadline = System.nanoTime() + timeout.toNanos();
      while (out.isEmpty() && System.nanoTime() < deadline) {
        consumer.poll(Duration.ofMillis(500)).forEach(out::add);
      }
    }
    return out;
  }
}
