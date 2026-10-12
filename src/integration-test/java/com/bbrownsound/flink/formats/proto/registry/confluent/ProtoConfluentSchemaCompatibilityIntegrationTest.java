package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration tests that walk a subject through a schema lifecycle against a real Confluent Schema
 * Registry: an initial schema, a compatible change, then a breaking change, across the registry's
 * compatibility modes and together with the dead-letter topic.
 *
 * <ul>
 *   <li>under {@code BACKWARD} a field addition registers and reads back, while a field type change
 *       is rejected by the registry (HTTP 409) and never becomes a version;
 *   <li>when a breaking change is forced through (compatibility {@code NONE}), a table still
 *       declared against the old schema cannot decode the new records: with {@code
 *       on-deserialize-error=skip} and a {@code dead-letter-topic} the job survives and the raw
 *       bytes of the breaking record land on the DLQ, with the default {@code fail} the job fails;
 *   <li>the compatibility modes accept and reject the changes they should, including the difference
 *       between {@code BACKWARD} and {@code BACKWARD_TRANSITIVE} over a subject's whole history;
 * </ul>
 */
@Testcontainers
@DisplayName("Proto-confluent schema compatibility lifecycle and dead-letter integration")
@Execution(ExecutionMode.SAME_THREAD)
class ProtoConfluentSchemaCompatibilityIntegrationTest {

  private static final String MESSAGE_NAME = "Order";

  private static final String V1 =
      "syntax = \"proto3\";\n"
          + "message Order {\n"
          + "  string order_id = 1;\n"
          + "  double amount = 2;\n"
          + "}\n";

  /** Compatible with v1: adds a field. */
  private static final String V2_ADDED_FIELD =
      "syntax = \"proto3\";\n"
          + "message Order {\n"
          + "  string order_id = 1;\n"
          + "  double amount = 2;\n"
          + "  string currency = 3;\n"
          + "}\n";

  /** Breaking against v1: field 2 changes from double to string. */
  private static final String V3_TYPE_CHANGE =
      "syntax = \"proto3\";\n"
          + "message Order {\n"
          + "  string order_id = 1;\n"
          + "  string amount = 2;\n"
          + "  string currency = 3;\n"
          + "}\n";

  private static final Network network = Network.newNetwork();

  @Container
  static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
          .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
          .withNetwork(network)
          .withExposedPorts(9093)
          .withNetworkAliases("kafka");

  @Container
  static GenericContainer<?> schemaRegistry =
      new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
          .withNetwork(network)
          .withExposedPorts(8081)
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
          .withEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "backward")
          .dependsOn(kafka)
          .waitingFor(
              Wait.forHttp("/subjects")
                  .forStatusCode(200)
                  .withStartupTimeout(Duration.ofSeconds(120)));

  static MiniClusterWithClientResource flinkCluster;
  static String bootstrapForJob;
  static String schemaRegistryUrl;
  static SchemaRegistryClient registryClient;

  @BeforeAll
  static void setup() throws Exception {
    final String bootstrapServers =
        String.format("PLAINTEXT://%s:%s", kafka.getHost(), kafka.getMappedPort(9093));
    schemaRegistryUrl =
        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    bootstrapForJob = bootstrapServers.substring("PLAINTEXT://".length());

    await()
        .atMost(120, SECONDS)
        .pollInterval(3, SECONDS)
        .until(
            () -> {
              try (AdminClient a =
                  AdminClient.create(
                      Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob))) {
                a.listTopics().listings().get(5, SECONDS);
                return true;
              } catch (InterruptedException
                  | ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                return false;
              }
            });

    registryClient =
        new CachedSchemaRegistryClient(
            schemaRegistryUrl,
            100,
            Collections.singletonList(new ProtobufSchemaProvider()),
            new HashMap<>());

    flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    flinkCluster.before();
  }

  @AfterAll
  static void teardown() {
    if (flinkCluster != null) {
      flinkCluster.after();
    }
  }

  @Test
  @DisplayName("BACKWARD: v1, then a compatible v2 reads back, then a breaking v3 is rejected")
  void backward_initialCompatibleThenBreaking() throws Exception {
    final String topic = "compat-it-lifecycle";
    final String subject = topic + "-value";
    createTopics(topic);
    registryClient.updateCompatibility(subject, "BACKWARD");

    final ProtobufSchema v1 = new ProtobufSchema(V1);
    final ProtobufSchema v2 = new ProtobufSchema(V2_ADDED_FIELD);
    final ProtobufSchema v3 = new ProtobufSchema(V3_TYPE_CHANGE);

    final int v1Id = registryClient.register(subject, v1);
    assertTrue(registryClient.testCompatibility(subject, v2), "added field must be compatible");
    final int v2Id = registryClient.register(subject, v2);
    assertNotEquals(v1Id, v2Id);

    assertFalse(registryClient.testCompatibility(subject, v3), "type change must be incompatible");
    final RestClientException rejected =
        assertThrows(RestClientException.class, () -> registryClient.register(subject, v3));
    assertEquals(409, rejected.getStatus(), "registry must answer 409 Conflict: " + rejected);
    assertEquals(
        List.of(1, 2),
        registryClient.getAllVersions(subject),
        "the breaking schema must not become a version");

    produce(topic, order(v1, "o-1", 5.0, null));
    produce(topic, order(v2, "o-2", 12.0, "USD"));

    // A table declared against v2 reads both writer schemas; currency is NULL for the v1 record.
    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE orders_v2 (order_id STRING, amount DOUBLE, currency STRING) WITH ("
            + sourceOptions(topic, "")
            + ")");
    final List<Row> rows = collect(tableEnv, "SELECT order_id, amount, currency FROM orders_v2");
    assertEquals(2, rows.size(), "both writer schemas must decode; got " + rows);
    assertNull(rowFor(rows, "o-1").getField(2), "v1 record has no currency column");
    assertEquals("USD", rowFor(rows, "o-2").getField(2));
  }

  @Test
  @DisplayName("NONE lets a breaking v3 in; skip + dead-letter-topic keeps the job alive")
  void breakingChange_poisonRecordGoesToDeadLetterTopic() throws Exception {
    final String topic = "compat-it-dlq";
    final String dlq = topic + "-dlq";
    final String subject = topic + "-value";
    createTopics(topic, dlq);

    final ProtobufSchema v1 = new ProtobufSchema(V1);
    final ProtobufSchema v3 = new ProtobufSchema(V3_TYPE_CHANGE);
    registryClient.updateCompatibility(subject, "NONE");
    registryClient.register(subject, v1);
    final int v3Id = registryClient.register(subject, v3);

    produce(topic, order(v1, "o-1", 5.0, null));
    produce(topic, stringAmountOrder(v3, "o-3", "9.99", "USD"));
    produce(topic, order(v1, "o-4", 7.5, null));

    // The consumer table is still declared against v1 (amount DOUBLE), so the v3 record's string
    // amount cannot be decoded: it is dropped and its raw bytes are kept on the DLQ.
    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE orders_v1 (order_id STRING, amount DOUBLE) WITH ("
            + sourceOptions(
                topic,
                "  'value.proto-confluent.on-deserialize-error' = 'skip',"
                    + "  'value.proto-confluent.dead-letter-topic' = '"
                    + dlq
                    + "',"
                    + "  'value.proto-confluent.dead-letter.properties' = 'bootstrap.servers:''"
                    + bootstrapForJob
                    + "''',")
            + ")");
    final List<Row> rows = collect(tableEnv, "SELECT order_id, amount FROM orders_v1");
    assertEquals(2, rows.size(), "only the v1 records survive; got " + rows);
    assertEquals(5.0, rowFor(rows, "o-1").getField(1));
    assertEquals(7.5, rowFor(rows, "o-4").getField(1));

    final List<ConsumerRecord<byte[], byte[]>> dead = consumeRecords(dlq, 1);
    assertEquals(1, dead.size(), "exactly the breaking record must be dead-lettered");
    final ConsumerRecord<byte[], byte[]> record = dead.get(0);
    assertEquals(v3Id, wireSchemaId(record.value()), "DLQ keeps the original wire bytes");
    final String errorMessage = header(record, "error.message");
    assertTrue(
        errorMessage.contains("Unsupported protobuf type") && errorMessage.contains("DOUBLE"),
        "error.message must name the type mismatch; got: " + errorMessage);
    assertEquals(topic, header(record, "source.topic"));
    assertTrue(header(record, "error.class").length() > 0, "error.class header must be set");
  }

  @Test
  @DisplayName("NONE lets a breaking v3 in; the default on-deserialize-error=fail stops the job")
  void breakingChange_failFastByDefault() throws Exception {
    final String topic = "compat-it-fail";
    final String subject = topic + "-value";
    createTopics(topic);

    final ProtobufSchema v1 = new ProtobufSchema(V1);
    final ProtobufSchema v3 = new ProtobufSchema(V3_TYPE_CHANGE);
    registryClient.updateCompatibility(subject, "NONE");
    registryClient.register(subject, v1);
    registryClient.register(subject, v3);
    produce(topic, stringAmountOrder(v3, "o-3", "9.99", "USD"));

    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE orders_v1 (order_id STRING, amount DOUBLE) WITH ("
            + sourceOptions(topic, "")
            + ")");
    final Exception failure =
        assertThrows(Exception.class, () -> collect(tableEnv, "SELECT order_id FROM orders_v1"));
    assertTrue(
        rootMessage(failure).contains("Unsupported protobuf type"),
        "the job must fail with the type mismatch; got: " + rootMessage(failure));
  }

  @Test
  @DisplayName("modes: additive change passes everywhere, type change only passes under NONE")
  void compatibilityModes_acceptAndRejectExpectedChanges() throws Exception {
    final ProtobufSchema v1 = new ProtobufSchema(V1);
    final ProtobufSchema added = new ProtobufSchema(V2_ADDED_FIELD);
    final ProtobufSchema breaking = new ProtobufSchema(V3_TYPE_CHANGE);

    final Map<String, Boolean> typeChangeAccepted = new HashMap<>();
    typeChangeAccepted.put("BACKWARD", false);
    typeChangeAccepted.put("FORWARD", false);
    typeChangeAccepted.put("FULL", false);
    typeChangeAccepted.put("NONE", true);

    for (Map.Entry<String, Boolean> mode : typeChangeAccepted.entrySet()) {
      final String subject = "compat-it-mode-" + mode.getKey().toLowerCase(Locale.ROOT) + "-value";
      registryClient.updateCompatibility(subject, "NONE");
      registryClient.register(subject, v1);
      registryClient.updateCompatibility(subject, mode.getKey());

      assertTrue(
          registryClient.testCompatibility(subject, added),
          mode.getKey() + " must accept an added field");
      assertEquals(
          mode.getValue(),
          registryClient.testCompatibility(subject, breaking),
          mode.getKey() + " type change verdict");
    }
  }

  @Test
  @DisplayName("BACKWARD checks the latest version only, BACKWARD_TRANSITIVE checks them all")
  void transitiveMode_checksWholeHistory() throws Exception {
    final ProtobufSchema v1 = new ProtobufSchema(V1);
    final ProtobufSchema breaking = new ProtobufSchema(V3_TYPE_CHANGE);
    // v4 only adds a field on top of the (already breaking) v3, so it is compatible with v3 but
    // still carries the type change relative to v1.
    final ProtobufSchema v4 =
        new ProtobufSchema(
            "syntax = \"proto3\";\n"
                + "message Order {\n"
                + "  string order_id = 1;\n"
                + "  string amount = 2;\n"
                + "  string currency = 3;\n"
                + "  string region = 4;\n"
                + "}\n");

    for (String mode : List.of("BACKWARD", "BACKWARD_TRANSITIVE")) {
      final String subject = "compat-it-history-" + mode.toLowerCase(Locale.ROOT) + "-value";
      registryClient.updateCompatibility(subject, "NONE");
      registryClient.register(subject, v1);
      registryClient.register(subject, breaking);
      registryClient.updateCompatibility(subject, mode);

      // The registry's /compatibility check against "latest" never looks past the newest version,
      // even in transitive modes, so it reports v4 compatible under BOTH modes. Only registration
      // applies the transitive rule. Guard this so a Registry upgrade that changes it is noticed
      // (see docs/content/how-to/schema-evolution.md).
      assertTrue(
          registryClient.testCompatibility(subject, v4),
          mode + ": the check against latest must pass (it ignores history)");
      // Checking against the oldest version explicitly is how to get the transitive verdict.
      assertFalse(
          testCompatibilityAgainstVersion(subject, v4, "1"),
          mode + ": the check against v1 must fail (type change relative to v1)");
      if ("BACKWARD".equals(mode)) {
        registryClient.register(subject, v4);
        assertEquals(List.of(1, 2, 3), registryClient.getAllVersions(subject));
      } else {
        final RestClientException rejected =
            assertThrows(RestClientException.class, () -> registryClient.register(subject, v4));
        assertEquals(409, rejected.getStatus(), mode + " must reject v4: " + rejected);
        assertEquals(List.of(1, 2), registryClient.getAllVersions(subject));
      }
    }
  }

  /** Calls POST /compatibility/subjects/{subject}/versions/{version} directly. */
  private static boolean testCompatibilityAgainstVersion(
      String subject, ProtobufSchema schema, String version) throws Exception {
    final HttpRequest request =
        HttpRequest.newBuilder(
                URI.create(
                    schemaRegistryUrl
                        + "/compatibility/subjects/"
                        + subject
                        + "/versions/"
                        + version))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .POST(HttpRequest.BodyPublishers.ofString(new RegisterSchemaRequest(schema).toJson()))
            .build();
    final HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    assertEquals(200, response.statusCode(), response.body());
    return response.body().contains("\"is_compatible\":true");
  }

  private static String sourceOptions(String topic, String extra) {
    return "  'connector' = 'kafka',"
        + "  'topic' = '"
        + topic
        + "',"
        + "  'properties.bootstrap.servers' = '"
        + bootstrapForJob
        + "',"
        + "  'properties.group.id' = 'compat-it-"
        + UUID.randomUUID()
        + "',"
        + "  'scan.startup.mode' = 'earliest-offset',"
        + "  'scan.bounded.mode' = 'latest-offset',"
        + "  'value.format' = 'proto-confluent',"
        + "  'value.proto-confluent.url' = '"
        + schemaRegistryUrl
        + "',"
        + "  'value.proto-confluent.topic' = '"
        + topic
        + "',"
        + extra
        + "  'value.proto-confluent.is_key' = 'false'";
  }

  private static DynamicMessage order(
      ProtobufSchema schema, String id, double amount, String currency) {
    final Descriptors.Descriptor d = schema.toDescriptor(MESSAGE_NAME);
    final DynamicMessage.Builder b =
        DynamicMessage.newBuilder(d)
            .setField(d.findFieldByName("order_id"), id)
            .setField(d.findFieldByName("amount"), amount);
    if (currency != null) {
      b.setField(d.findFieldByName("currency"), currency);
    }
    return b.build();
  }

  private static DynamicMessage stringAmountOrder(
      ProtobufSchema schema, String id, String amount, String currency) {
    final Descriptors.Descriptor d = schema.toDescriptor(MESSAGE_NAME);
    return DynamicMessage.newBuilder(d)
        .setField(d.findFieldByName("order_id"), id)
        .setField(d.findFieldByName("amount"), amount)
        .setField(d.findFieldByName("currency"), currency)
        .build();
  }

  private static StreamTableEnvironment newTableEnv() {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    return StreamTableEnvironment.create(env);
  }

  private static List<Row> collect(StreamTableEnvironment tableEnv, String query) throws Exception {
    final List<Row> rows = new ArrayList<>();
    final TableResult result = tableEnv.executeSql(query);
    try (CloseableIterator<Row> it = result.collect()) {
      while (it.hasNext()) {
        rows.add(it.next());
      }
    }
    return rows;
  }

  private static Row rowFor(List<Row> rows, String orderId) {
    return rows.stream()
        .filter(r -> orderId.equals(String.valueOf(r.getField(0))))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no row for '" + orderId + "' in " + rows));
  }

  /** Reads the schema ID out of the Confluent wire header (magic byte + 4-byte big-endian ID). */
  private static int wireSchemaId(byte[] payload) throws Exception {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
      in.readByte();
      return in.readInt();
    }
  }

  private static String header(ConsumerRecord<byte[], byte[]> record, String name) {
    final Header h = record.headers().lastHeader(name);
    assertTrue(h != null, name + " header must be present");
    return new String(h.value(), StandardCharsets.UTF_8);
  }

  private static String rootMessage(Throwable t) {
    final StringBuilder sb = new StringBuilder();
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      sb.append(cur.getClass().getName()).append(": ").append(cur.getMessage()).append(" | ");
      if (cur.getCause() == cur) {
        break;
      }
    }
    return sb.toString();
  }

  private static void produce(String topic, DynamicMessage message) throws Exception {
    final Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", bootstrapForJob);
    config.put("key.serializer", StringSerializer.class.getName());
    config.put("value.serializer", KafkaProtobufSerializer.class.getName());
    config.put("schema.registry.url", schemaRegistryUrl);
    // The test registers every schema itself so each record's wire schema ID is deterministic.
    config.put("auto.register.schemas", "false");
    config.put("use.latest.version", "false");
    try (KafkaProducer<String, DynamicMessage> producer = new KafkaProducer<>(config)) {
      producer.send(new ProducerRecord<>(topic, message)).get(30, SECONDS);
      producer.flush();
    }
  }

  private static List<byte[]> consume(String topic, int expected) {
    final List<byte[]> out = new ArrayList<>();
    consumeRecords(topic, expected).forEach(r -> out.add(r.value()));
    return out;
  }

  private static List<ConsumerRecord<byte[], byte[]>> consumeRecords(String topic, int expected) {
    final Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "compat-it-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      await()
          .atMost(60, SECONDS)
          .pollInterval(Duration.ofSeconds(1))
          .until(
              () -> {
                final ConsumerRecords<byte[], byte[]> records =
                    consumer.poll(Duration.ofMillis(1000));
                records.forEach(
                    r -> {
                      if (r.value() != null) {
                        out.add(r);
                      }
                    });
                return out.size() >= expected;
              });
    }
    return out;
  }

  private static void createTopics(String... topics) throws Exception {
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob))) {
      final List<NewTopic> newTopics = new ArrayList<>();
      for (String topic : topics) {
        newTopics.add(new NewTopic(topic, 1, (short) 1));
      }
      admin.createTopics(newTopics).all().get(60, SECONDS);
    }
  }
}
