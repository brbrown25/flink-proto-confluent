package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.cli.ProtoConfluentCli;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A recursive message (issue #216) read and written through derived DDL against a real Kafka and
 * Schema Registry: the catalog, with each {@code recursive-message} mode, exposes the topic as a
 * table whose {@code INSERT INTO} writes back what it reads ({@code bytes} and {@code json} every
 * level, {@code row:<n>} the levels the row has), and the statement the {@code ddl} command prints
 * with {@code --recursive-message} reads the topic.
 */
@DisplayName("Recursive messages through derived DDL against a real Kafka and Schema Registry")
class RecursiveMessageIntegrationTest extends ProtoConfluentTestBase {

  private static final String TOPIC = "trees";
  private static final String TREE_SCHEMA =
      "syntax = \"proto3\";\npackage rec;\nmessage Tree {\n  string name = 1;\n  Tree child = 2;\n"
          + "  repeated Tree children = 3;\n}";

  private static MiniClusterWithClientResource flinkCluster;
  private static String bootstrap;
  private static Descriptor tree;
  private static int schemaId;

  @BeforeAll
  static void setUp() throws Exception {
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
  static void tearDown() {
    if (flinkCluster != null) {
      flinkCluster.after();
    }
  }

  /** Registers the schema and produces one tree, once for the whole class. */
  private void prepareOnce() throws Exception {
    synchronized (RecursiveMessageIntegrationTest.class) {
      if (schemaId != 0) {
        return;
      }
      ProtobufSchema schema = new ProtobufSchema(TREE_SCHEMA);
      schemaId = getSchemaRegistryClient().register(TOPIC + "-value", schema);
      tree = schema.toDescriptor();
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
      try (KafkaProducer<byte[], byte[]> producer =
          new KafkaProducer<>(
              Map.of(
                  ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                  ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                  ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                      ByteArraySerializer.class.getName()))) {
        producer.send(new ProducerRecord<>(TOPIC, wireFormat(deepTree("a")))).get(30, SECONDS);
      }
    }
  }

  private static DynamicMessage.Builder node(String name) {
    return DynamicMessage.newBuilder(tree).setField(tree.findFieldByName("name"), name);
  }

  private static DynamicMessage.Builder node(String name, Message child) {
    return node(name).setField(tree.findFieldByName("child"), child);
  }

  private static Message withChildren(DynamicMessage.Builder builder, Message... children) {
    FieldDescriptor field = tree.findFieldByName("children");
    for (Message child : children) {
      builder.addRepeatedField(field, child);
    }
    return builder.build();
  }

  /** Four levels through {@code child} and two through {@code children}. */
  private static Message deepTree(String name) {
    return withChildren(
        node(name, node("b", node("c", node("d").build()).build()).build()),
        node("x", node("y").build()).build());
  }

  /** Confluent wire format: magic byte, schema ID, message index of the first message, payload. */
  private static byte[] wireFormat(Message message) {
    byte[] payload = message.toByteArray();
    return ByteBuffer.allocate(1 + 4 + 1 + payload.length)
        .put((byte) 0)
        .putInt(schemaId)
        .put((byte) 0)
        .put(payload)
        .array();
  }

  private StreamTableEnvironment tableEnv(String recursion) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(
        "CREATE CATALOG sr WITH (\n"
            + "  'type' = 'proto-confluent',\n"
            + "  'url' = '" + getSchemaRegistryUrl() + "',\n"
            + "  'recursive-message' = '" + recursion + "',\n"
            + "  'properties.bootstrap.servers' = '" + bootstrap + "',\n"
            + "  'properties.group.id' = 'recursive-it-" + UUID.randomUUID() + "',\n"
            + "  'scan.startup.mode' = 'earliest-offset',\n"
            + "  'scan.bounded.mode' = 'latest-offset'\n"
            + ")");
    return tableEnv;
  }

  private static List<Row> collect(StreamTableEnvironment tableEnv, String query)
      throws Exception {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> it = tableEnv.executeSql(query).collect()) {
      it.forEachRemaining(rows::add);
    }
    return rows;
  }

  /** Copies the produced tree through the catalog table, renamed, and returns what was written. */
  private Message copyThroughTheCatalog(String recursion, String name) throws Exception {
    prepareOnce();
    StreamTableEnvironment tableEnv = tableEnv(recursion);
    tableEnv
        .executeSql(
            "INSERT INTO sr.`default`.trees SELECT '" + name + "', child, children"
                + " FROM sr.`default`.trees WHERE name = 'a'")
        .await(120, SECONDS);
    return readTopic().get(name);
  }

  /** Reads every record of the topic, by tree name. */
  private static Map<String, Message> readTopic() throws Exception {
    Map<String, Message> byName = new HashMap<>();
    try (KafkaConsumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName()))) {
      TopicPartition partition = new TopicPartition(TOPIC, 0);
      consumer.assign(List.of(partition));
      consumer.seekToBeginning(List.of(partition));
      long end = consumer.endOffsets(List.of(partition)).get(partition);
      while (consumer.position(partition) < end) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
          byte[] value = record.value();
          ByteBuffer buffer = ByteBuffer.wrap(value);
          assertEquals(0, buffer.get(), "magic byte");
          assertEquals(schemaId, buffer.getInt(), "the sink writes with the registered schema");
          // Message indexes: a single 0 for the first message.
          assertEquals(0, buffer.get());
          Message message =
              DynamicMessage.parseFrom(tree, Arrays.copyOfRange(value, 6, value.length));
          byName.put((String) message.getField(tree.findFieldByName("name")), message);
        }
      }
    }
    return byName;
  }

  @Test
  @DisplayName("without 'recursive-message' the table fails to load and names the option")
  void failsByDefault() throws Exception {
    prepareOnce();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(
        "CREATE CATALOG sr WITH ('type' = 'proto-confluent', 'url' = '"
            + getSchemaRegistryUrl()
            + "')");
    Exception e =
        org.junit.jupiter.api.Assertions.assertThrows(
            Exception.class, () -> tableEnv.executeSql("SELECT * FROM sr.`default`.trees"));
    StringBuilder messages = new StringBuilder();
    for (Throwable t = e; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    assertTrue(messages.toString().contains("'recursive-message'"), messages.toString());
  }

  @Test
  @DisplayName("'recursive-message' = 'bytes' reads and writes every level")
  void bytesRoundTrips() throws Exception {
    prepareOnce();
    assertEquals(deepTree("a-bytes"), copyThroughTheCatalog("bytes", "a-bytes"));
  }

  @Test
  @DisplayName("'recursive-message' = 'json' reads and writes every level")
  void jsonRoundTrips() throws Exception {
    prepareOnce();
    List<Row> rows =
        collect(tableEnv("json"), "SELECT child FROM sr.`default`.trees WHERE name = 'a'");
    assertEquals(
        "{\"name\":\"b\",\"child\":{\"name\":\"c\",\"child\":{\"name\":\"d\"}}}",
        rows.get(0).getField(0));
    assertEquals(deepTree("a-json"), copyThroughTheCatalog("json", "a-json"));
  }

  @Test
  @DisplayName("'recursive-message' = 'row:2' reads and writes two levels below the table's message")
  void rowRoundTripsTheLevelsItKeeps() throws Exception {
    prepareOnce();
    List<Row> rows =
        collect(
            tableEnv("row:2"),
            "SELECT child.name, child.child.name, children[1].child.name"
                + " FROM sr.`default`.trees WHERE name = 'a'");
    assertEquals(Row.of("b", "c", "y"), rows.get(0));
    assertEquals(
        withChildren(
            node("a-row", node("b", node("c").build()).build()), node("x", node("y").build()).build()),
        copyThroughTheCatalog("row:2", "a-row"));
  }

  @Test
  @DisplayName("the statement printed with --recursive-message bytes reads the topic")
  void ddlCommandStatementReadsTheTopic() throws Exception {
    prepareOnce();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int status =
        new ProtoConfluentCli(
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8))
            .run(
                "ddl",
                "--url", getSchemaRegistryUrl(),
                "--subject", TOPIC + "-value",
                "--table", "trees_ddl",
                "--bootstrap-servers", bootstrap,
                "--recursive-message", "bytes",
                "--option", "properties.group.id=recursive-ddl-" + UUID.randomUUID(),
                "--option", "scan.startup.mode=earliest-offset",
                "--option", "scan.bounded.mode=latest-offset");
    assertEquals(0, status, err.toString(StandardCharsets.UTF_8));
    String statement = out.toString(StandardCharsets.UTF_8).trim();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(statement.substring(0, statement.length() - 1));
    List<Row> rows = collect(tableEnv, "SELECT child FROM trees_ddl WHERE name = 'a'");
    Message child = (Message) deepTree("a").getField(tree.findFieldByName("child"));
    assertArrayEquals(child.toByteArray(), (byte[]) rows.get(0).getField(0));
  }
}
