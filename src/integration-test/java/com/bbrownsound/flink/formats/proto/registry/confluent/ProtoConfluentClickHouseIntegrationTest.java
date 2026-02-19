package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
 * Integration test: Testcontainers (Kafka + Schema Registry + ClickHouse) produce protobuf to a
 * topic; MiniCluster runs a Flink job that reads from Kafka (proto-confluent), applies a simple
 * transform, and sinks to ClickHouse via a JDBC sink. Uses executeAsync(), polls ClickHouse until
 * expected rows, cancels the job, then asserts. Same pattern as
 * MaxMeasurementsObservationsIntegrationTest.
 */
@Testcontainers
@DisplayName(
    "Proto-confluent ClickHouse integration: produce -> Kafka -> Flink transform -> ClickHouse")
@Execution(
    ExecutionMode
        .SAME_THREAD) // so getExecutionEnvironment() sees MiniCluster context set in BeforeAll
class ProtoConfluentClickHouseIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoConfluentClickHouseIntegrationTest.class);

  private static void log(String msg) {
    LOG.info("[ClickHouseIT] {}", msg);
  }

  private static final String TOPIC = "integration-clickhouse";
  private static final String CLICKHOUSE_DB = "flink_test";
  private static final String CLICKHOUSE_TABLE = "simple_sink";
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

  private static final String CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.6.12.10";

  @Container
  static GenericContainer<?> clickHouse =
      new GenericContainer<>(DockerImageName.parse(CLICKHOUSE_IMAGE))
          .withNetwork(network)
          .withNetworkAliases("clickhouse")
          .withExposedPorts(8123)
          .withEnv("CLICKHOUSE_SKIP_USER_SETUP", "1");

  static MiniClusterWithClientResource flinkCluster;
  static String bootstrapServers;
  static String schemaRegistryUrl;
  static String clickHouseJdbcUrl;

  @BeforeAll
  static void setup() throws Exception {
    log("setup: starting");
    bootstrapServers =
        String.format("PLAINTEXT://%s:%s", kafka.getHost(), kafka.getMappedPort(9093));
    schemaRegistryUrl =
        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
    log("setup: waiting for Kafka...");
    clickHouseJdbcUrl =
        "jdbc:clickhouse:http://" + clickHouse.getHost() + ":" + clickHouse.getMappedPort(8123);
    try {
      Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("ClickHouse JDBC driver not on classpath", e);
    }

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
    log("setup: topic ready, waiting for ClickHouse...");

    await().atMost(30, SECONDS).until(ProtoConfluentClickHouseIntegrationTest::clickHouseReady);
    log("setup: ClickHouse ready, creating DB and table");
    try (Connection conn = DriverManager.getConnection(clickHouseJdbcUrl);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + CLICKHOUSE_DB);
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS "
              + CLICKHOUSE_DB
              + "."
              + CLICKHOUSE_TABLE
              + " (content String, date_time String) ENGINE = MergeTree() ORDER BY content");
    }
    log("setup: starting Flink MiniCluster");
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

  private static boolean clickHouseReady() {
    try (Connection c = DriverManager.getConnection(clickHouseJdbcUrl)) {
      return true;
    } catch (Exception e) {
      return false;
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
  @Timeout(value = 120, unit = SECONDS)
  void selectWithTransformAndSinkToClickHouse() throws Exception {
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
                      .setContent("a")
                      .setDateTime("2025-01-01")
                      .build()))
          .get(10, SECONDS);
      producer
          .send(
              new ProducerRecord<>(
                  TOPIC,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("b")
                      .setDateTime("2025-01-02")
                      .build()))
          .get(10, SECONDS);
      producer
          .send(
              new ProducerRecord<>(
                  TOPIC,
                  TestSimple.SimpleMessage.newBuilder()
                      .setContent("c")
                      .setDateTime("2025-01-03")
                      .build()))
          .get(10, SECONDS);
      producer.flush();
    }
    log("test: produced 3 messages to Kafka");

    await().pollDelay(java.time.Duration.ofSeconds(2)).until(() -> true);

    log("test: creating Flink env, source table, and pipeline with ClickHouse sink");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(
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
            + ")");

    Table result =
        tableEnv.sqlQuery("SELECT content, UPPER(date_time) AS date_time FROM simple_src");
    String tableRef = CLICKHOUSE_DB + "." + CLICKHOUSE_TABLE;
    tableEnv
        .toDataStream(result)
        .addSink(new ClickHouseJdbcSink(clickHouseJdbcUrl, tableRef))
        .name("ClickHouse " + tableRef);

    log(
        "test: starting job (executeAsync), bootstrap="
            + bootstrapForJob
            + ", schemaRegistry="
            + schemaRegistryUrl);
    JobClient jobClient = env.executeAsync("Proto-confluent Kafka to ClickHouse");
    log(
        "test: job submitted, jobId="
            + jobClient.getJobID()
            + ", will poll ClickHouse every 2s for up to 90s...");

    await()
        .atMost(90, SECONDS)
        .pollInterval(java.time.Duration.ofSeconds(2))
        .until(
            () -> {
              int n = getClickHouseRowCount();
              log("test: poll ClickHouse row count=" + n);
              return n >= 3;
            });
    int rowCount = getClickHouseRowCount();
    log("test: reached " + rowCount + " rows, cancelling job...");
    jobClient.cancel();
    assertTrue(
        rowCount >= 3, "ClickHouse should have at least 3 rows within 90s (had " + rowCount + ")");
    try {
      jobClient.getJobExecutionResult().get(30, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof JobCancellationException) {
        // Expected when we cancel
      } else {
        throw e;
      }
    }

    log("test: verifying ClickHouse data");
    try (Connection conn = getClickHouseConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT content, date_time FROM "
                    + CLICKHOUSE_DB
                    + "."
                    + CLICKHOUSE_TABLE
                    + " ORDER BY content")) {
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertEquals("2025-01-01", rs.getString(2));
      assertTrue(rs.next());
      assertEquals("b", rs.getString(1));
      assertEquals("2025-01-02", rs.getString(2));
      assertTrue(rs.next());
      assertEquals("c", rs.getString(1));
      assertEquals("2025-01-03", rs.getString(2));
    }
    log("test: done");
  }

  /**
   * Sink that writes Flink Row (content, date_time) to ClickHouse via JDBC. Used so the pipeline
   * has a real sink and we can use executeAsync() + poll + cancel like
   * MaxMeasurementsObservationsIntegrationTest.
   */
  private static final class ClickHouseJdbcSink extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;
    private final String jdbcUrl;
    private final String tableRef;
    private transient Connection connection;
    private transient PreparedStatement statement;

    ClickHouseJdbcSink(String jdbcUrl, String tableRef) {
      this.jdbcUrl = jdbcUrl;
      this.tableRef = tableRef;
    }

    private transient int invokeCount = 0;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
      super.open(parameters);
      try {
        LOG.debug("[ClickHouseSink] open: url={}, table={}", jdbcUrl, tableRef);
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        connection = DriverManager.getConnection(jdbcUrl);
        statement =
            connection.prepareStatement(
                "INSERT INTO " + tableRef + " (content, date_time) VALUES (?, ?)");
        LOG.debug("[ClickHouseSink] open: connection and statement ready");
      } catch (Exception e) {
        LOG.error(
            "[ClickHouseSink] open: FAILED - {}: {}", e.getClass().getSimpleName(), e.getMessage());
        throw e;
      }
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
      try {
        statement.setString(1, (String) row.getField(0));
        statement.setString(2, (String) row.getField(1));
        statement.executeUpdate();
        invokeCount++;
        if (invokeCount <= 5 || invokeCount % 10 == 0) {
          LOG.debug("[ClickHouseSink] invoke: row {} written", invokeCount);
        }
      } catch (Exception e) {
        LOG.error(
            "[ClickHouseSink] invoke: FAILED - {}: {}",
            e.getClass().getSimpleName(),
            e.getMessage());
        throw e;
      }
    }

    @Override
    public void close() throws Exception {
      LOG.debug("[ClickHouseSink] close: total rows written={}", invokeCount);
      if (statement != null) {
        statement.close();
      }
      if (connection != null) {
        connection.close();
      }
      super.close();
    }
  }

  private Connection getClickHouseConnection() throws java.sql.SQLException {
    try {
      Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
    } catch (ClassNotFoundException e) {
      throw new java.sql.SQLException("ClickHouse JDBC driver not on classpath", e);
    }
    return DriverManager.getConnection(clickHouseJdbcUrl);
  }

  private int getClickHouseRowCount() {
    try (Connection conn = getClickHouseConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery("SELECT count() FROM " + CLICKHOUSE_DB + "." + CLICKHOUSE_TABLE)) {
      return rs.next() ? rs.getInt(1) : 0;
    } catch (Exception e) {
      return 0;
    }
  }
}
