package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.OrderSchemas;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.ResolvedProtoSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistrySchemaResolver;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The Schema Registry catalog (issue #169) against a real Kafka and Schema Registry: a topic with a
 * registered protobuf value (and key) schema is listed and queried without a {@code CREATE TABLE},
 * hints override the catalog's table options, other schema types are skipped, and an {@code INSERT
 * INTO} a catalog table writes records the catalog reads back.
 */
@DisplayName("Schema Registry catalog against a real Kafka and Schema Registry")
class SchemaRegistryCatalogIntegrationTest extends ProtoConfluentTestBase {

  private static final String TOPIC = "orders";
  private static final String KEY_SCHEMA =
      "syntax = \"proto3\";\npackage ddl.orders;\nmessage OrderKey {\n  string order_id = 1;\n}";

  private static MiniClusterWithClientResource flinkCluster;
  private static String bootstrap;
  private static boolean prepared;

  private StreamTableEnvironment tableEnv;

  @BeforeAll
  static void setUpCluster() throws Exception {
    bootstrap = kafka.getHost() + ":" + kafka.getMappedPort(9093);
    flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    flinkCluster.before();
  }

  @AfterAll
  static void tearDownCluster() {
    if (flinkCluster != null) {
      flinkCluster.after();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    prepareOnce();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(
        "CREATE CATALOG sr WITH (\n"
            + "  'type' = 'proto-confluent',\n"
            + "  'url' = '"
            + getSchemaRegistryUrl()
            + "',\n"
            + "  'properties.bootstrap.servers' = '"
            + bootstrap
            + "',\n"
            + "  'properties.group.id' = 'catalog-it-"
            + UUID.randomUUID()
            + "',\n"
            + "  'scan.startup.mode' = 'earliest-offset',\n"
            + "  'scan.bounded.mode' = 'latest-offset'\n"
            + ")");
  }

  /**
   * Registers the order value and key schemas, an Avro and a JSON Schema subject, and produces two
   * keyed orders, once for the whole class.
   */
  private void prepareOnce() throws Exception {
    synchronized (SchemaRegistryCatalogIntegrationTest.class) {
      if (prepared) {
        return;
      }
      // The referenced schema is registered under its import path, as Confluent serializers do by
      // default (DefaultReferenceSubjectNameStrategy), so the sink can resolve it when it writes.
      OrderSchemas.register(getSchemaRegistryClient(), OrderSchemas.MONEY_IMPORT, TOPIC + "-value");
      getSchemaRegistryClient().register(TOPIC + "-key", new ProtobufSchema(KEY_SCHEMA));
      RestService rest = new RestService(getSchemaRegistryUrl());
      rest.registerSchema(
          "{\"type\":\"record\",\"name\":\"Click\",\"fields\":[{\"name\":\"url\",\"type\":\"string\"}]}",
          "AVRO",
          List.of(),
          "clicks-value");
      rest.registerSchema(
          "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}",
          "JSON",
          List.of(),
          "users-value");
      createTopic();
      produceOrders();
      prepared = true;
    }
  }

  private static void createTopic() {
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      await()
          .atMost(Duration.ofSeconds(120))
          .pollInterval(Duration.ofSeconds(2))
          .ignoreExceptions()
          .until(() -> {
            if (!admin.listTopics().names().get(30, SECONDS).contains(TOPIC)) {
              admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(30, SECONDS);
            }
            return admin.listTopics().names().get(30, SECONDS).contains(TOPIC);
          });
    }
  }

  private void produceOrders() throws Exception {
    SchemaRegistrySchemaResolver resolver =
        SchemaRegistrySchemaResolver.create(getSchemaRegistryUrl(), Map.of());
    ResolvedProtoSchema value = resolver.resolve(TOPIC + "-value");
    ResolvedProtoSchema key = resolver.resolve(TOPIC + "-key");
    try (KafkaProducer<byte[], byte[]> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName()))) {
      for (String[] order : new String[][] {{"o-1", "2"}, {"o-2", "5"}}) {
        producer
            .send(
                new ProducerRecord<>(
                    TOPIC,
                    wireFormat(key.getId(), orderKey(key.getDescriptor(), order[0])),
                    wireFormat(
                        value.getId(),
                        order(value.getDescriptor(), order[0], Integer.parseInt(order[1])))))
            .get(30, SECONDS);
      }
    }
  }

  private static Message orderKey(Descriptor key, String id) {
    return DynamicMessage.newBuilder(key).setField(key.findFieldByName("order_id"), id).build();
  }

  private static Message order(Descriptor order, String id, int quantity) {
    Descriptor customer = order.findNestedTypeByName("Customer");
    Descriptor money = order.findFieldByName("total").getMessageType();
    return DynamicMessage.newBuilder(order)
        .setField(order.findFieldByName("order_id"), id)
        .setField(
            order.findFieldByName("customer"),
            DynamicMessage.newBuilder(customer)
                .setField(customer.findFieldByName("name"), "acme-" + id)
                .build())
        .setField(
            order.findFieldByName("total"),
            DynamicMessage.newBuilder(money)
                .setField(money.findFieldByName("currency"), "EUR")
                .setField(money.findFieldByName("units"), 10L * quantity)
                .build())
        .addRepeatedField(order.findFieldByName("tags"), "t-" + id)
        .build();
  }

  /** Confluent wire format: magic byte, schema ID, message index of the first message, payload. */
  private static byte[] wireFormat(int schemaId, Message message) {
    byte[] payload = message.toByteArray();
    return ByteBuffer.allocate(1 + 4 + 1 + payload.length)
        .put((byte) 0)
        .putInt(schemaId)
        .put((byte) 0)
        .put(payload)
        .array();
  }

  private List<Row> collect(String query) throws Exception {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> it = tableEnv.executeSql(query).collect()) {
      it.forEachRemaining(rows::add);
    }
    rows.sort(Comparator.comparing(Row::toString));
    return rows;
  }

  @Test
  @DisplayName("SHOW TABLES lists protobuf topics and skips Avro and JSON Schema subjects")
  void showTablesListsProtobufTopicsOnly() throws Exception {
    List<String> tables = new ArrayList<>();
    for (Row row : collect("SHOW TABLES FROM sr.`default`")) {
      tables.add(String.valueOf(row.getField(0)));
    }
    assertEquals(List.of(TOPIC), tables);

    assertEquals(
        List.of("default"),
        collect("SHOW DATABASES IN sr").stream().map(r -> String.valueOf(r.getField(0))).toList());
    Exception avro =
        assertThrows(
            Exception.class, () -> tableEnv.executeSql("SELECT * FROM sr.`default`.clicks"));
    assertTrue(avro.getMessage().contains("clicks"), avro.getMessage());
  }

  @Test
  @DisplayName("SELECT * FROM sr.`default`.orders reads the topic, key columns first")
  void selectReadsTheTopic() throws Exception {
    List<Row> rows = collect("SELECT * FROM sr.`default`.orders WHERE order_id <> 'o-3'");
    assertEquals(2, rows.size(), rows.toString());
    Row first = rows.get(0);
    assertEquals(7, first.getArity(), "key_order_id plus the six value columns: " + first);

    List<Row> projected =
        collect(
            "SELECT key_order_id, order_id, customer.name, total.units FROM sr.`default`.orders"
                + " WHERE order_id <> 'o-3'");
    assertEquals(
        List.of(Row.of("o-1", "o-1", "acme-o-1", 20L), Row.of("o-2", "o-2", "acme-o-2", 50L)),
        projected);
  }

  @Test
  @DisplayName("an OPTIONS hint overrides a table option the catalog set")
  void hintOverridesCatalogOptions() throws Exception {
    List<Row> rows =
        collect(
            "SELECT order_id FROM sr.`default`.orders /*+ OPTIONS("
                + "'scan.startup.mode' = 'specific-offsets', "
                + "'scan.startup.specific-offsets' = 'partition:0,offset:1', "
                + "'scan.bounded.mode' = 'specific-offsets', "
                + "'scan.bounded.specific-offsets' = 'partition:0,offset:2') */");
    assertEquals(List.of(Row.of("o-2")), rows, "the hint skips the first record");
  }

  @Test
  @DisplayName("CREATE TABLE in the catalog is rejected as read-only")
  void createTableIsRejected() {
    Exception e =
        assertThrows(
            Exception.class,
            () ->
                tableEnv.executeSql(
                    "CREATE TABLE sr.`default`.t (a INT) WITH ('connector' = 'datagen')"));
    StringBuilder messages = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    assertTrue(messages.toString().contains("read-only"), messages.toString());
  }

  @Test
  @DisplayName("INSERT INTO a catalog table writes with the registered key and value schemas")
  void insertIntoACatalogTableWrites() throws Exception {
    TableResult insert =
        tableEnv.executeSql(
            "INSERT INTO sr.`default`.orders "
                + "SELECT 'o-3', 'o-3', customer, items, attributes, total, tags "
                + "FROM sr.`default`.orders WHERE order_id = 'o-1'");
    insert.await(120, SECONDS);

    List<Row> rows =
        collect(
            "SELECT key_order_id, order_id, customer.name, total.units FROM sr.`default`.orders"
                + " WHERE order_id = 'o-3'");
    assertEquals(List.of(Row.of("o-3", "o-3", "acme-o-1", 20L)), rows);
    assertFalse(
        getSchemaRegistryClient().getAllSubjects().stream()
            .anyMatch(s ->
                s.startsWith(TOPIC) && !s.equals(TOPIC + "-value") && !s.equals(TOPIC + "-key")),
        "the sink registers no new subject");
    assertEquals(
        1, getSchemaRegistryClient().getAllVersions(TOPIC + "-value").size(), "nor a new version");
  }
}
