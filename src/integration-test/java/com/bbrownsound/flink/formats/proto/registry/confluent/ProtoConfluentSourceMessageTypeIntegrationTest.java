package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.UnexpectedMessageTypeException;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.Message;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
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
 * Integration test for GitHub issue #177: a source honours {@code message-class} and {@code
 * message-name}. A table that does not match {@code message-class} is rejected when the query is
 * planned, naming the offending column; records of another message type on the topic follow {@code
 * on-deserialize-error} (the job fails by default; with {@code skip} they are dropped and
 * dead-lettered); and records of the expected message are still read.
 */
@Testcontainers
@DisplayName("Proto-confluent source message-class / message-name integration")
@Execution(ExecutionMode.SAME_THREAD)
class ProtoConfluentSourceMessageTypeIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoConfluentSourceMessageTypeIntegrationTest.class);

  private static final String SIMPLE_CLASS = TestSimple.SimpleMessage.class.getName();
  private static final String SIMPLE_NAME = TestSimple.SimpleMessage.getDescriptor().getFullName();
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
                  .withStartupTimeout(Duration.ofSeconds(120)));

  static MiniClusterWithClientResource flinkCluster;
  static String bootstrap;
  static String schemaRegistryUrl;

  @BeforeAll
  static void setup() throws Exception {
    bootstrap = kafka.getHost() + ":" + kafka.getMappedPort(9093);
    schemaRegistryUrl =
        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    await()
        .atMost(120, SECONDS)
        .pollInterval(3, SECONDS)
        .until(() -> {
          try (AdminClient a =
              AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            a.listTopics().listings().get(5, SECONDS);
            return true;
          } catch (InterruptedException
              | ExecutionException
              | java.util.concurrent.TimeoutException e) {
            return false;
          }
        });
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
  void messageClass_tableThatDoesNotMatch_isRejectedNamingTheColumn() {
    StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE mismatched (`content` STRING, `order_id` STRING) WITH ("
            + source("mt-it-mismatch")
            + ", 'value.proto-confluent.message-class' = '"
            + SIMPLE_CLASS
            + "')");
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> tableEnv.explainSql("SELECT * FROM mismatched"));
    String message = findMessageInCauseChain(e, "column 'order_id'");
    assertNotNull(message, "No cause names the offending column: " + e);
    assertTrue(message.contains("has no field in message '" + SIMPLE_NAME + "'"), message);
    assertTrue(message.contains("'message-class' = '" + SIMPLE_CLASS + "'"), message);

    tableEnv.executeSql(
        "CREATE TABLE wrong_type (`content` INT) WITH ("
            + source("mt-it-mismatch")
            + ", 'value.proto-confluent.message-class' = '"
            + SIMPLE_CLASS
            + "')");
    e =
        assertThrows(
            ValidationException.class, () -> tableEnv.explainSql("SELECT * FROM wrong_type"));
    message = findMessageInCauseChain(e, "column 'content' is INT");
    assertNotNull(message, "No cause names the mistyped column: " + e);
  }

  @Test
  void messageName_unexpectedMessageType_failsTheJobByDefault() throws Exception {
    String topic = "mt-it-fail";
    createTopic(topic);
    produce(topic, simple("first"));
    produce(topic, otherMessage());

    StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE expect_simple (`content` STRING, `date_time` STRING) WITH ("
            + source(topic)
            + ", 'value.proto-confluent.message-name' = '"
            + SIMPLE_NAME
            + "')");
    tableEnv.executeSql(
        "CREATE TABLE fail_sink (`content` STRING, `date_time` STRING)"
            + " WITH ('connector' = 'blackhole')");
    TableResult result = tableEnv.executeSql("INSERT INTO fail_sink SELECT * FROM expect_simple");
    JobClient jobClient = result.getJobClient().orElseThrow();
    try {
      await()
          .atMost(120, SECONDS)
          .pollInterval(Duration.ofSeconds(1))
          .until(() -> jobClient.getJobStatus().get(30, SECONDS) == JobStatus.FAILED);
      ExecutionException failure =
          assertThrows(
              ExecutionException.class, () -> jobClient.getJobExecutionResult().get(60, SECONDS));
      String message = findMessageInCauseChain(failure, "but the source expects");
      assertNotNull(message, "The job must fail on the unexpected message type: " + failure);
      assertTrue(
          message.contains("'" + TestMap.MapTest.getDescriptor().getFullName() + "'"), message);
      assertTrue(message.contains("expects '" + SIMPLE_NAME + "'"), message);
    } finally {
      cancelQuietly(result);
    }
  }

  @Test
  void messageClass_unexpectedMessageTypeSkippedAndDeadLettered_expectedOnesStillRead()
      throws Exception {
    String topic = "mt-it-skip";
    String dlqTopic = "mt-it-skip-dlq";
    createTopic(topic);
    createTopic(dlqTopic);
    produce(topic, simple("first"));
    produce(topic, otherMessage());
    produce(topic, simple("second"));

    StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE skip_src (`content` STRING, `date_time` STRING) WITH ("
            + source(topic)
            + ", 'value.proto-confluent.message-class' = '"
            + SIMPLE_CLASS
            + "', 'value.proto-confluent.on-deserialize-error' = 'skip'"
            + ", 'value.proto-confluent.dead-letter-topic' = '"
            + dlqTopic
            + "')");

    TableResult result = tableEnv.executeSql("SELECT `content` FROM skip_src");
    List<String> contents = new ArrayList<>();
    try (CloseableIterator<Row> rows = result.collect()) {
      while (contents.size() < 2 && rows.hasNext()) {
        contents.add((String) rows.next().getField(0));
      }
    }
    assertEquals(List.of("first", "second"), contents);

    await()
        .atMost(60, SECONDS)
        .pollInterval(Duration.ofSeconds(2))
        .until(() -> !consume(dlqTopic).isEmpty());
    ConsumerRecord<byte[], byte[]> dead = consume(dlqTopic).get(0);
    Header errorClass = dead.headers().lastHeader("error.class");
    assertNotNull(errorClass, "error.class header must be present");
    assertEquals(
        UnexpectedMessageTypeException.class.getName(),
        new String(errorClass.value(), StandardCharsets.UTF_8));
    LOG.info("test: unexpected message type skipped and dead-lettered");
  }

  private static StreamTableEnvironment newTableEnv() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    return StreamTableEnvironment.create(env);
  }

  private static String source(String topic) {
    return "'connector' = 'kafka',"
        + " 'topic' = '"
        + topic
        + "',"
        + " 'properties.bootstrap.servers' = '"
        + bootstrap
        + "',"
        + " 'properties.group.id' = 'mt-it-"
        + UUID.randomUUID()
        + "',"
        + " 'scan.startup.mode' = 'earliest-offset',"
        + " 'value.format' = 'proto-confluent',"
        + " 'value.proto-confluent.url' = '"
        + schemaRegistryUrl
        + "',"
        + " 'value.proto-confluent.topic' = '"
        + topic
        + "'";
  }

  private static TestSimple.SimpleMessage simple(String content) {
    return TestSimple.SimpleMessage.newBuilder()
        .setContent(content)
        .setDateTime("2025-01-01")
        .build();
  }

  /** A different message whose map happens to carry a 'content' key. */
  private static TestMap.MapTest otherMessage() {
    return TestMap.MapTest.newBuilder().putSimpleStringMap("content", "not-a-simple").build();
  }

  /**
   * Produces with {@code TopicRecordNameStrategy}, so each message type gets its own subject and
   * the registry's compatibility check does not stop two types sharing the topic.
   */
  private static void produce(String topic, Message message) throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", bootstrap);
    config.put("key.serializer", StringSerializer.class.getName());
    config.put("value.serializer", KafkaProtobufSerializer.class.getName());
    config.put("schema.registry.url", schemaRegistryUrl);
    config.put("auto.register.schemas", "true");
    config.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());
    try (KafkaProducer<String, Message> producer = new KafkaProducer<>(config)) {
      producer.send(new ProducerRecord<>(topic, message)).get(10, SECONDS);
    }
  }

  private static void createTopic(String topic) throws Exception {
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(60, SECONDS);
      await()
          .atMost(35, SECONDS)
          .pollInterval(Duration.ofSeconds(1))
          .until(() ->
              admin
                  .describeTopics(Collections.singletonList(topic))
                  .allTopicNames()
                  .get(10, SECONDS)
                  .containsKey(topic));
    }
  }

  private static List<ConsumerRecord<byte[], byte[]>> consume(String topic) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "mt-it-check-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      consumer.poll(Duration.ofMillis(5000)).forEach(out::add);
    }
    return out;
  }

  private static String findMessageInCauseChain(Throwable error, String fragment) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t.getMessage() != null && t.getMessage().contains(fragment)) {
        return t.getMessage();
      }
    }
    return null;
  }

  private static void cancelQuietly(TableResult result) {
    result
        .getJobClient()
        .ifPresent(jc -> {
          try {
            jc.cancel().get(30, TimeUnit.SECONDS);
          } catch (InterruptedException
              | ExecutionException
              | java.util.concurrent.TimeoutException e) {
            LOG.debug("[MessageTypeIT] job already finished", e);
          }
        });
  }
}
