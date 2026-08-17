package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
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
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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
 * Integration test: a poison (non-proto) record on the source topic, with {@code
 * on-deserialize-error=skip} and a {@code dead-letter-topic} configured, is dropped instead of
 * crashing the job; a well-formed record on the same topic still flows through end to end; and the
 * poison record's raw bytes land on the dead-letter topic with the {@code error.class}, {@code
 * error.message} and {@code source.topic} headers. Covers GitHub issue #69.
 */
@Testcontainers
@DisplayName("Proto-confluent dead-letter topic + skip-keeps-job-alive integration")
@Execution(ExecutionMode.SAME_THREAD)
class ProtoConfluentDeadLetterTopicIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoConfluentDeadLetterTopicIntegrationTest.class);

  private static final String SOURCE_TOPIC = "dlq-it-source";
  private static final String DLQ_TOPIC = "dlq-it-dead-letter";
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
  static String bootstrapForJob;
  static String schemaRegistryUrl;

  @BeforeAll
  static void setup() throws Exception {
    String bootstrapServers =
        String.format("PLAINTEXT://%s:%s", kafka.getHost(), kafka.getMappedPort(9093));
    schemaRegistryUrl =
        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    bootstrapForJob =
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
                      Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob))) {
                a.listTopics().listings().get(5, SECONDS);
                return true;
              } catch (InterruptedException
                  | ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                return false;
              }
            });

    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob))) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(SOURCE_TOPIC, 1, (short) 1), new NewTopic(DLQ_TOPIC, 1, (short) 1)))
          .all()
          .get(60, SECONDS);
      awaitKafkaTopicReady(admin, SOURCE_TOPIC);
      awaitKafkaTopicReady(admin, DLQ_TOPIC);
    }

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

  private static void awaitKafkaTopicReady(AdminClient admin, String topic) {
    await()
        .atMost(35, SECONDS)
        .pollInterval(Duration.ofSeconds(1))
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
                  | ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                return false;
              }
            });
  }

  @Test
  void poisonRecordSkippedAndDeadLettered_wellFormedRecordStillFlows() throws Exception {
    // Produce one well-formed proto message and one poison (non-proto) record to the source
    // topic. Both are byte-array values on the wire; the well-formed one is proto-confluent
    // encoded via KafkaProtobufSerializer, the poison one is arbitrary garbage bytes.
    Map<String, Object> protoProducerConfig =
        Map.of(
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
        new KafkaProducer<>(protoProducerConfig)) {
      producer
          .send(
              new ProducerRecord<>(
                  SOURCE_TOPIC,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("still-alive")
                      .setDateTime("2025-01-01")
                      .build()))
          .get(10, SECONDS);
      producer.flush();
    }

    byte[] poison = new byte[] {0, 1, 2, 3, 4};
    Map<String, Object> rawProducerConfig =
        Map.of(
            "bootstrap.servers",
            bootstrapForJob,
            "key.serializer",
            ByteArraySerializer.class.getName(),
            "value.serializer",
            ByteArraySerializer.class.getName());
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(rawProducerConfig)) {
      producer.send(new ProducerRecord<>(SOURCE_TOPIC, null, poison)).get(10, SECONDS);
      producer.flush();
    }
    LOG.info("test: produced 1 well-formed and 1 poison record to {}", SOURCE_TOPIC);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    String createSrc =
        "CREATE TABLE dlq_src ("
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + SOURCE_TOPIC
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
            + SOURCE_TOPIC
            + "',"
            + "  'value.proto-confluent.is_key' = 'false',"
            + "  'value.proto-confluent.on-deserialize-error' = 'skip',"
            + "  'value.proto-confluent.dead-letter-topic' = '"
            + DLQ_TOPIC
            + "',"
            // Map-type options split each "k:v" pair on ':'; since bootstrapForJob is itself
            // "host:port", the value must be quoted (per Flink's StructuredOptionsSplitter) so
            // the extra colon isn't mistaken for another key/value separator. The single quotes
            // are doubled here because the whole thing is itself a SQL string literal.
            + "  'value.proto-confluent.dead-letter.properties' = 'bootstrap.servers:''"
            + bootstrapForJob
            + "'''"
            + ")";
    tableEnv.executeSql(createSrc);

    String sinkTopic = "dlq-it-sink";
    createTopic(sinkTopic);
    String createSink =
        "CREATE TABLE dlq_sink ("
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

    TableResult insertResult = tableEnv.executeSql("INSERT INTO dlq_sink SELECT * FROM dlq_src");
    try {
      // The job must stay alive (not crash-loop) despite the poison record, and the well-formed
      // record must still make it to the sink.
      await()
          .atMost(90, SECONDS)
          .pollInterval(Duration.ofSeconds(2))
          .until(() -> !consumeSinkRecords(sinkTopic).isEmpty());

      List<byte[]> sinkValues = consumeSinkRecords(sinkTopic);
      assertTrue(sinkValues.size() >= 1, "Expected at least 1 record on the sink topic");

      // The poison record's raw bytes must land on the dead-letter topic with error headers.
      await()
          .atMost(90, SECONDS)
          .pollInterval(Duration.ofSeconds(2))
          .until(() -> !consumeDeadLetterRecords().isEmpty());

      List<ConsumerRecord<byte[], byte[]>> dlqRecords = consumeDeadLetterRecords();
      assertTrue(dlqRecords.size() >= 1, "Expected at least 1 record on the dead-letter topic");
      ConsumerRecord<byte[], byte[]> dlqRecord = dlqRecords.get(0);
      assertEquals(poison.length, dlqRecord.value().length);

      Header errorClass = dlqRecord.headers().lastHeader("error.class");
      Header errorMessage = dlqRecord.headers().lastHeader("error.message");
      Header sourceTopicHeader = dlqRecord.headers().lastHeader("source.topic");
      assertTrue(errorClass != null, "error.class header must be present");
      assertTrue(errorMessage != null, "error.message header must be present");
      assertTrue(sourceTopicHeader != null, "source.topic header must be present");
      assertEquals(SOURCE_TOPIC, new String(sourceTopicHeader.value(), StandardCharsets.UTF_8));
      LOG.info("test: done - job stayed alive, well-formed record delivered, poison dead-lettered");
    } finally {
      cancelQuietly(insertResult);
    }
  }

  private static void cancelQuietly(TableResult result) {
    result
        .getJobClient()
        .ifPresent(
            jc -> {
              try {
                jc.cancel().get(30, TimeUnit.SECONDS);
              } catch (InterruptedException
                  | ExecutionException
                  | java.util.concurrent.TimeoutException e) {
                LOG.warn("[DeadLetterIT] failed to cancel job cleanly", e);
              }
            });
  }

  private void createTopic(String topic) throws Exception {
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob))) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(60, SECONDS);
      awaitKafkaTopicReady(admin, topic);
    }
  }

  private List<byte[]> consumeSinkRecords(String topic) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "sink-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<byte[]> out = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
      records.forEach(
          r -> {
            if (r.value() != null) {
              out.add(r.value());
            }
          });
    }
    return out;
  }

  private List<ConsumerRecord<byte[], byte[]>> consumeDeadLetterRecords() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(DLQ_TOPIC));
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
      records.forEach(out::add);
    }
    return out;
  }
}
