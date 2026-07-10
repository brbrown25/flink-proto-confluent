package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration test: Testcontainers (Kafka + Schema Registry) produce protobuf data to a topic; then
 * MiniCluster runs a Flink job that reads from the topic (proto-confluent), sinks to an in-memory
 * collection. Uses executeAsync(), polls collection size, cancels, then asserts. Avoids
 * executeAndCollect.
 */
@Testcontainers
@DisplayName("Proto-confluent MiniCluster integration: produce -> Kafka -> Flink SELECT")
@Execution(
    ExecutionMode
        .SAME_THREAD) // so getExecutionEnvironment() sees MiniCluster context set in BeforeAll
class ProtoConfluentMiniClusterIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoConfluentMiniClusterIntegrationTest.class);

  private static void log(String msg) {
    LOG.info("[MiniClusterIT] {}", msg);
  }

  private static final String TOPIC = "integration-simple";
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
          .dependsOn(kafka)
          .waitingFor(
              Wait.forHttp("/subjects")
                  .forStatusCode(200)
                  .withStartupTimeout(java.time.Duration.ofSeconds(120)));

  static MiniClusterWithClientResource flinkCluster;

  /**
   * Registry so the sink (after deserialization in the task) looks up the same queue the test
   * polls.
   */
  private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<Row>> COLLECT_SINK_REGISTRY =
      new ConcurrentHashMap<>();

  /** Key for this test's collection queue. */
  private static final String COLLECT_KEY = "simpleSelect";

  static String bootstrapServers;
  static String schemaRegistryUrl;

  @BeforeAll
  static void setup() throws Exception {
    log("setup: starting");
    bootstrapServers =
        String.format("PLAINTEXT://%s:%s", kafka.getHost(), kafka.getMappedPort(9093));
    schemaRegistryUrl =
        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    log("setup: waiting for Kafka...");

    String bootstrapForAdmin =
        bootstrapServers.startsWith("PLAINTEXT://")
            ? bootstrapServers.substring("PLAINTEXT://".length())
            : bootstrapServers;
    await()
        .atMost(120, SECONDS)
        .pollInterval(3, SECONDS)
        .until(
            () -> {
              try (AdminClient a =
                  AdminClient.create(
                      Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForAdmin))) {
                a.listTopics().listings().get(5, SECONDS);
                return true;
              } catch (InterruptedException
                  | java.util.concurrent.ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                return false;
              }
            });
    log("setup: Kafka ready, creating topic " + TOPIC);

    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForAdmin))) {
      admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(60, SECONDS);
      awaitKafkaTopicReady(admin, TOPIC);
    }
    log("setup: topic ready, starting Flink MiniCluster");
    flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(2)
                .build());
    flinkCluster.before();
    log("setup: done");
  }

  @AfterAll
  static void teardown() {
    if (flinkCluster != null) {
      flinkCluster.after();
    }
  }

  private static void awaitKafkaTopicReady(AdminClient admin, String topic) {
    await()
        .atMost(35, SECONDS)
        .pollInterval(java.time.Duration.ofSeconds(1))
        .until(
            () -> {
              try {
                Map<String, TopicDescription> desc =
                    admin
                        .describeTopics(Collections.singletonList(topic))
                        .allTopicNames()
                        .get(10, SECONDS);
                return desc != null && desc.containsKey(topic);
              } catch (InterruptedException
                  | java.util.concurrent.ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                return false;
              }
            });
  }

  @Test
  void simpleSelectFromProtoConfluentTopic() throws Exception {
    log("test: starting");
    String bootstrapForJob =
        bootstrapServers.startsWith("PLAINTEXT://")
            ? bootstrapServers.substring("PLAINTEXT://".length())
            : bootstrapServers;
    Map<String, Object> producerConfig =
        Map.<String, Object>of(
            "bootstrap.servers",
            bootstrapForJob,
            "key.serializer",
            StringSerializer.class.getName(),
            "value.serializer",
            KafkaProtobufSerializer.class.getName(),
            "schema.registry.url",
            schemaRegistryUrl,
            "auto.register.schemas",
            "true");
    try (KafkaProducer<String, TestSimple.SimpleMessage> producer =
        new KafkaProducer<>(producerConfig)) {
      producer
          .send(
              new ProducerRecord<>(
                  TOPIC,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("hello")
                      .setDateTime("2025-01-01")
                      .build()))
          .get(10, SECONDS);
      producer
          .send(
              new ProducerRecord<>(
                  TOPIC,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("world")
                      .setDateTime("2025-01-02")
                      .build()))
          .get(10, SECONDS);
      producer.flush();
    }
    log("test: produced 2 messages to Kafka");

    ConcurrentLinkedQueue<Row> collectedRows = new ConcurrentLinkedQueue<>();
    COLLECT_SINK_REGISTRY.put(COLLECT_KEY, collectedRows);
    try {
      await().pollDelay(java.time.Duration.ofSeconds(2)).until(() -> true);

      log("test: creating Flink env and table, pipeline with collection sink");
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

      String createTable =
          "CREATE TABLE simple_src ("
              + "  `content` STRING,"
              + "  `date_time` STRING"
              + ") WITH ("
              + "  'connector' = 'kafka',"
              + "  'topic' = '"
              + TOPIC
              + "',"
              + "  'properties.bootstrap.servers' = '"
              + bootstrapForJob
              + "',"
              + "  'scan.startup.mode' = 'earliest-offset',"
              + "  'value.format' = 'proto-confluent',"
              + "  'value.proto-confluent.url' = '"
              + schemaRegistryUrl
              + "',"
              + "  'value.proto-confluent.topic' = '"
              + TOPIC
              + "',"
              + "  'value.proto-confluent.is_key' = 'false'"
              + ")";
      tableEnv.executeSql(createTable);

      Table table = tableEnv.from("simple_src");
      tableEnv.toDataStream(table).addSink(new CollectSink(COLLECT_KEY)).name("collect-rows");

      log(
          "test: starting job (executeAsync), bootstrap="
              + bootstrapForJob
              + ", schemaRegistry="
              + schemaRegistryUrl);
      JobClient jobClient = env.executeAsync("Proto-confluent Kafka SELECT");
      log(
          "test: job submitted, jobId="
              + jobClient.getJobID()
              + ", will poll collection every 2s for up to 90s...");

      await()
          .atMost(90, SECONDS)
          .pollInterval(java.time.Duration.ofSeconds(2))
          .until(
              () -> {
                int n = collectedRows.size();
                log("test: poll collectedRows.size()=" + n);
                return n >= 2;
              });
      int count = collectedRows.size();
      log("test: collected " + count + " rows, cancelling job...");
      jobClient.cancel();
      try {
        jobClient.getJobExecutionResult().get(30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof JobCancellationException)) {
          throw e;
        }
      }

      List<Row> rows = new ArrayList<>(collectedRows);
      assertTrue(rows.size() >= 2, "Expected at least 2 rows; got " + rows.size());
      Row first = rows.get(0);
      assertEquals("hello", first.getField(0).toString());
      assertEquals("2025-01-01", first.getField(1).toString());
      log("test: done");
    } finally {
      COLLECT_SINK_REGISTRY.remove(COLLECT_KEY);
    }
  }

  /**
   * Sink with dynamic (Row-derived) schema: no value.message-class. Flink writes to Kafka using
   * proto-confluent; schema is the Row type. Consumer reads as DynamicMessage and verifies content.
   */
  @Test
  void sinkDynamicSchema_insertIntoKafka_consumableAsDynamicMessage() throws Exception {
    String sinkTopic = "integration-simple-sink-dynamic";
    String bootstrapForJob =
        bootstrapServers.startsWith("PLAINTEXT://")
            ? bootstrapServers.substring("PLAINTEXT://".length())
            : bootstrapServers;
    createTopic(sinkTopic, bootstrapForJob);
    produceTwoMessagesToTopic(TOPIC, bootstrapForJob);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    String createSrc =
        "CREATE TABLE simple_src ("
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + TOPIC
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'scan.startup.mode' = 'earliest-offset',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + TOPIC
            + "',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")";
    tableEnv.executeSql(createSrc);

    String createSink =
        "CREATE TABLE simple_sink ("
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + sinkTopic
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + sinkTopic
            + "',"
            + "  'value.proto-confluent.auto-register-schemas' = 'true',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")";
    tableEnv.executeSql(createSink);

    org.apache.flink.table.api.TableResult insertResult =
        tableEnv.executeSql("INSERT INTO simple_sink SELECT * FROM simple_src");

    try {
      await()
          .atMost(90, SECONDS)
          .pollInterval(Duration.ofSeconds(2))
          .until(
              () -> {
                List<DynamicMessage> consumed = consumeAsDynamicMessage(sinkTopic, bootstrapForJob);
                return consumed.size() >= 2;
              });

      List<DynamicMessage> messages = consumeAsDynamicMessage(sinkTopic, bootstrapForJob);
      assertTrue(messages.size() >= 2, "Expected at least 2 messages; got " + messages.size());
      DynamicMessage first = messages.get(0);
      assertEquals(
          "hello",
          first.getField(first.getDescriptorForType().findFieldByName("content")).toString());
      assertEquals(
          "2025-01-01",
          first.getField(first.getDescriptorForType().findFieldByName("date_time")).toString());
      log("sinkDynamicSchema: done");
    } finally {
      cancelQuietly(insertResult);
    }
  }

  /**
   * Sink with value.message-class: Flink writes using SimpleMessage schema. Consumer deserializes
   * as SPECIFIC_PROTOBUF_VALUE_TYPE=SimpleMessage and verifies typed accessors.
   */
  @Test
  void sinkWithMessageClass_insertIntoKafka_consumableAsSimpleMessage() throws Exception {
    String sinkTopic = "integration-simple-sink-message-class";
    String bootstrapForJob =
        bootstrapServers.startsWith("PLAINTEXT://")
            ? bootstrapServers.substring("PLAINTEXT://".length())
            : bootstrapServers;
    createTopic(sinkTopic, bootstrapForJob);
    produceTwoMessagesToTopic(TOPIC, bootstrapForJob);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    String createSrc =
        "CREATE TABLE simple_src ("
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + TOPIC
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'scan.startup.mode' = 'earliest-offset',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + TOPIC
            + "',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")";
    tableEnv.executeSql(createSrc);

    String createSink =
        "CREATE TABLE simple_sink ("
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + sinkTopic
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + sinkTopic
            + "',"
            + "  'value.proto-confluent.auto-register-schemas' = 'true',"
            + "  'value.proto-confluent.properties' = 'value.message-class:"
            + TestSimple.SimpleMessage.class.getName()
            + "',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")";
    tableEnv.executeSql(createSink);

    org.apache.flink.table.api.TableResult insertResult =
        tableEnv.executeSql("INSERT INTO simple_sink SELECT * FROM simple_src");

    try {
      await()
          .atMost(90, SECONDS)
          .pollInterval(Duration.ofSeconds(2))
          .until(
              () -> {
                List<TestSimple.SimpleMessage> consumed =
                    consumeAsSimpleMessage(sinkTopic, bootstrapForJob);
                return consumed.size() >= 2;
              });

      List<TestSimple.SimpleMessage> messages = consumeAsSimpleMessage(sinkTopic, bootstrapForJob);
      assertTrue(messages.size() >= 2, "Expected at least 2 messages; got " + messages.size());
      TestSimple.SimpleMessage first = messages.get(0);
      assertEquals("hello", first.getContent());
      assertEquals("2025-01-01", first.getDateTime());
      log("sinkWithMessageClass: done");
    } finally {
      cancelQuietly(insertResult);
    }
  }

  /**
   * Source with a strongly typed proto key AND value: produce records whose key and value are both
   * SimpleMessage protos, then read them through a Flink table that declares 'key.format' and
   * 'value.format' as proto-confluent. Verifies the strongly typed key is parsed into the prefixed
   * key columns alongside the value columns. Covers GitHub issue #4.
   */
  @Test
  void keyedSource_readsProtoKeyAndValue() throws Exception {
    String keyedTopic = "integration-keyed-src";
    String bootstrapForJob =
        bootstrapServers.startsWith("PLAINTEXT://")
            ? bootstrapServers.substring("PLAINTEXT://".length())
            : bootstrapServers;
    createTopic(keyedTopic, bootstrapForJob);
    produceKeyedMessages(keyedTopic, bootstrapForJob);

    String collectKey = "keyedSelect";
    ConcurrentLinkedQueue<Row> collectedRows = new ConcurrentLinkedQueue<>();
    COLLECT_SINK_REGISTRY.put(collectKey, collectedRows);
    try {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

      // key.fields-prefix strips 'k_' before the key format sees the columns, so the key format's
      // Row type is (content, date_time) and matches the SimpleMessage descriptor by field name.
      String createTable =
          "CREATE TABLE keyed_src ("
              + "  `k_content` STRING,"
              + "  `k_date_time` STRING,"
              + "  `content` STRING,"
              + "  `date_time` STRING"
              + ") WITH ("
              + "  'connector' = 'kafka',"
              + "  'topic' = '"
              + keyedTopic
              + "',"
              + "  'properties.bootstrap.servers' = '"
              + bootstrapForJob
              + "',"
              + "  'scan.startup.mode' = 'earliest-offset',"
              + "  'key.format' = 'proto-confluent',"
              + "  'key.fields' = 'k_content;k_date_time',"
              + "  'key.fields-prefix' = 'k_',"
              + "  'key.proto-confluent.url' = '"
              + schemaRegistryUrl
              + "',"
              + "  'key.proto-confluent.topic' = '"
              + keyedTopic
              + "',"
              + "  'key.proto-confluent.is_key' = 'true',"
              + "  'value.format' = 'proto-confluent',"
              + "  'value.fields-include' = 'EXCEPT_KEY',"
              + "  'value.proto-confluent.url' = '"
              + schemaRegistryUrl
              + "',"
              + "  'value.proto-confluent.topic' = '"
              + keyedTopic
              + "',"
              + "  'value.proto-confluent.is_key' = 'false'"
              + ")";
      tableEnv.executeSql(createTable);

      Table table = tableEnv.from("keyed_src");
      tableEnv.toDataStream(table).addSink(new CollectSink(collectKey)).name("collect-keyed");

      JobClient jobClient = env.executeAsync("Proto-confluent keyed SELECT");
      await()
          .atMost(90, SECONDS)
          .pollInterval(Duration.ofSeconds(2))
          .until(() -> collectedRows.size() >= 2);
      jobClient.cancel();
      try {
        jobClient.getJobExecutionResult().get(30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof JobCancellationException)) {
          throw e;
        }
      }

      List<Row> rows = new ArrayList<>(collectedRows);
      assertTrue(rows.size() >= 2, "Expected at least 2 keyed rows; got " + rows.size());
      Row first = rows.get(0);
      // Columns: k_content, k_date_time (from the proto key), content, date_time (from the value).
      assertEquals("key-hello", first.getField(0).toString());
      assertEquals("2025-07-01", first.getField(1).toString());
      assertEquals("hello", first.getField(2).toString());
      assertEquals("2025-01-01", first.getField(3).toString());
      log("keyedSource: done");
    } finally {
      COLLECT_SINK_REGISTRY.remove(collectKey);
    }
  }

  /**
   * Sink pinning an explicit key message class via 'key.proto-confluent.message-class'. Flink
   * writes the key using the named SimpleMessage entity (not a dynamic schema); the consumer
   * deserializes the key as SPECIFIC_PROTOBUF_KEY_TYPE=SimpleMessage and verifies typed accessors.
   * Covers GitHub issue #20.
   */
  @Test
  void sinkWithKeyMessageClass_keyConsumableAsSimpleMessage() throws Exception {
    String sinkTopic = "integration-keyed-sink-message-class";
    String bootstrapForJob =
        bootstrapServers.startsWith("PLAINTEXT://")
            ? bootstrapServers.substring("PLAINTEXT://".length())
            : bootstrapServers;
    createTopic(sinkTopic, bootstrapForJob);
    produceTwoMessagesToTopic(TOPIC, bootstrapForJob);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    String createSrc =
        "CREATE TABLE simple_src_for_keyed_sink ("
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + TOPIC
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'scan.startup.mode' = 'earliest-offset',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + TOPIC
            + "',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")";
    tableEnv.executeSql(createSrc);

    String createSink =
        "CREATE TABLE keyed_sink ("
            + "  `k_content` STRING,"
            + "  `k_date_time` STRING,"
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + sinkTopic
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'key.format' = 'proto-confluent',"
            + "  'key.fields' = 'k_content;k_date_time',"
            + "  'key.fields-prefix' = 'k_',"
            + "  'key.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'key.proto-confluent.topic' = '"
            + sinkTopic
            + "',"
            + "  'key.proto-confluent.auto-register-schemas' = 'true',"
            + "  'key.proto-confluent.is_key' = 'true',"
            + "  'key.proto-confluent.message-class' = '"
            + TestSimple.SimpleMessage.class.getName()
            + "',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.fields-include' = 'EXCEPT_KEY',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + sinkTopic
            + "',"
            + "  'value.proto-confluent.auto-register-schemas' = 'true',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")";
    tableEnv.executeSql(createSink);

    // Capture the streaming INSERT job so it can be cancelled: the shared MiniCluster has only 2
    // task slots, so a leaked (never-cancelled) sink job would starve later tests of slots.
    org.apache.flink.table.api.TableResult insertResult =
        tableEnv.executeSql(
            "INSERT INTO keyed_sink SELECT content, date_time, content, date_time "
                + "FROM simple_src_for_keyed_sink");

    try {
      await()
          .atMost(90, SECONDS)
          .pollInterval(Duration.ofSeconds(2))
          .until(() -> consumeKeyAsSimpleMessage(sinkTopic, bootstrapForJob).size() >= 2);

      List<TestSimple.SimpleMessage> keys = consumeKeyAsSimpleMessage(sinkTopic, bootstrapForJob);
      assertTrue(keys.size() >= 2, "Expected at least 2 keys; got " + keys.size());
      TestSimple.SimpleMessage firstKey = keys.get(0);
      assertEquals("hello", firstKey.getContent());
      assertEquals("2025-01-01", firstKey.getDateTime());
      log("sinkWithKeyMessageClass: done");
    } finally {
      cancelQuietly(insertResult);
    }
  }

  private static void cancelQuietly(org.apache.flink.table.api.TableResult result) {
    result
        .getJobClient()
        .ifPresent(
            jc -> {
              try {
                jc.cancel().get(30, TimeUnit.SECONDS);
              } catch (InterruptedException
                  | ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                LOG.warn("[MiniClusterIT] failed to cancel job cleanly", e);
              }
            });
  }

  private void produceKeyedMessages(String topic, String bootstrapForJob) throws Exception {
    Map<String, Object> producerConfig =
        Map.<String, Object>of(
            "bootstrap.servers",
            bootstrapForJob,
            "key.serializer",
            KafkaProtobufSerializer.class.getName(),
            "value.serializer",
            KafkaProtobufSerializer.class.getName(),
            "schema.registry.url",
            schemaRegistryUrl,
            "auto.register.schemas",
            "true");
    try (KafkaProducer<TestSimple.SimpleMessage, TestSimple.SimpleMessage> producer =
        new KafkaProducer<>(producerConfig)) {
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("key-hello")
                      .setDateTime("2025-07-01")
                      .build(),
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("hello")
                      .setDateTime("2025-01-01")
                      .build()))
          .get(10, SECONDS);
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("key-world")
                      .setDateTime("2025-07-02")
                      .build(),
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("world")
                      .setDateTime("2025-01-02")
                      .build()))
          .get(10, SECONDS);
      producer.flush();
    }
  }

  private List<TestSimple.SimpleMessage> consumeKeyAsSimpleMessage(String topic, String bootstrap) {
    Map<String, Object> props = new java.util.HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "key-specific-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(
        KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_KEY_TYPE,
        TestSimple.SimpleMessage.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<TestSimple.SimpleMessage> out = new ArrayList<>();
    try (KafkaConsumer<TestSimple.SimpleMessage, DynamicMessage> consumer =
        new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      ConsumerRecords<TestSimple.SimpleMessage, DynamicMessage> records =
          consumer.poll(Duration.ofMillis(5000));
      records.forEach(
          r -> {
            if (r.key() != null) out.add(r.key());
          });
    }
    return out;
  }

  private void createTopic(String topic, String bootstrapForJob) throws Exception {
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob))) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(60, SECONDS);
      awaitKafkaTopicReady(admin, topic);
    }
  }

  private void produceTwoMessagesToTopic(String topic, String bootstrapForJob) throws Exception {
    Map<String, Object> producerConfig =
        Map.<String, Object>of(
            "bootstrap.servers",
            bootstrapForJob,
            "key.serializer",
            StringSerializer.class.getName(),
            "value.serializer",
            KafkaProtobufSerializer.class.getName(),
            "schema.registry.url",
            schemaRegistryUrl,
            "auto.register.schemas",
            "true");
    try (KafkaProducer<String, TestSimple.SimpleMessage> producer =
        new KafkaProducer<>(producerConfig)) {
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("hello")
                      .setDateTime("2025-01-01")
                      .build()))
          .get(10, SECONDS);
      producer
          .send(
              new ProducerRecord<>(
                  topic,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("world")
                      .setDateTime("2025-01-02")
                      .build()))
          .get(10, SECONDS);
      producer.flush();
    }
  }

  private List<DynamicMessage> consumeAsDynamicMessage(String topic, String bootstrap) {
    Map<String, Object> props = new java.util.HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dynamic-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<DynamicMessage> out = new ArrayList<>();
    try (KafkaConsumer<String, DynamicMessage> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      ConsumerRecords<String, DynamicMessage> records = consumer.poll(Duration.ofMillis(5000));
      records.forEach(
          r -> {
            if (r.value() != null) out.add(r.value());
          });
    }
    return out;
  }

  private List<TestSimple.SimpleMessage> consumeAsSimpleMessage(String topic, String bootstrap) {
    Map<String, Object> props = new java.util.HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "specific-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(
        KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
        TestSimple.SimpleMessage.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<TestSimple.SimpleMessage> out = new ArrayList<>();
    try (KafkaConsumer<String, TestSimple.SimpleMessage> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      ConsumerRecords<String, TestSimple.SimpleMessage> records =
          consumer.poll(Duration.ofMillis(5000));
      records.forEach(
          r -> {
            if (r.value() != null) out.add(r.value());
          });
    }
    return out;
  }

  private static final class CollectSink extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;
    private final String registryKey;
    private transient ConcurrentLinkedQueue<Row> target;
    private transient int invokeCount = 0;

    CollectSink(String registryKey) {
      this.registryKey = registryKey;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
      super.open(parameters);
      target = COLLECT_SINK_REGISTRY.get(registryKey);
      if (target == null) {
        throw new IllegalStateException("No queue registered for key: " + registryKey);
      }
      LOG.debug("[CollectSink] open: sink ready");
    }

    @Override
    public void invoke(Row row, Context context) {
      target.add(row);
      invokeCount++;
      if (invokeCount <= 5 || invokeCount % 10 == 0) {
        LOG.debug("[CollectSink] invoke: row {} collected", invokeCount);
      }
    }

    @Override
    public void close() throws Exception {
      LOG.debug("[CollectSink] close: total rows collected={}", invokeCount);
      super.close();
    }
  }
}
