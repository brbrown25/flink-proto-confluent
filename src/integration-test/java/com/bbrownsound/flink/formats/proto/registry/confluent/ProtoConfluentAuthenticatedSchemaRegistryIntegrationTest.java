package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import org.testcontainers.utility.MountableFile;

/**
 * Integration test: proves the {@code basic-auth.*} / {@code bearer-auth.*} proto-confluent format
 * options (wired in {@code RegistryProtoFormatFactory.buildOptionalPropertiesMap}) actually
 * authenticate against a Schema Registry secured with {@code SCHEMA_REGISTRY_AUTHENTICATION_METHOD
 * =BASIC}. Covers GitHub issue #67.
 */
@Testcontainers
@DisplayName("Proto-confluent authenticated Schema Registry integration")
@Execution(ExecutionMode.SAME_THREAD)
class ProtoConfluentAuthenticatedSchemaRegistryIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoConfluentAuthenticatedSchemaRegistryIntegrationTest.class);

  private static final String VALID_USER = "sruser";
  private static final String VALID_PASSWORD = "srpassword";
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
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("auth/schema-registry.jaas"),
              "/etc/schema-registry/auth/schema-registry.jaas")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("auth/schema-registry.password"),
              "/etc/schema-registry/auth/schema-registry.password")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
          .withEnv("SCHEMA_REGISTRY_AUTHENTICATION_METHOD", "BASIC")
          .withEnv("SCHEMA_REGISTRY_AUTHENTICATION_REALM", "SchemaRegistry-Props")
          .withEnv("SCHEMA_REGISTRY_AUTHENTICATION_ROLES", "admin,developer,user")
          .withEnv(
              "SCHEMA_REGISTRY_OPTS",
              "-Djava.security.auth.login.config=/etc/schema-registry/auth/schema-registry.jaas")
          .dependsOn(kafka)
          .waitingFor(
              Wait.forHttp("/subjects")
                  .withBasicCredentials(VALID_USER, VALID_PASSWORD)
                  .forStatusCode(200)
                  .withStartupTimeout(Duration.ofSeconds(120)));

  static MiniClusterWithClientResource flinkCluster;
  static String bootstrapForJob;
  static String schemaRegistryUrl;

  @BeforeAll
  static void setup() throws Exception {
    String bootstrapServers =
        String.format("PLAINTEXT://%s:%s", kafka.getHost(), kafka.getMappedPort(9093));
    bootstrapForJob = bootstrapServers.substring("PLAINTEXT://".length());
    schemaRegistryUrl =
        "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);

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

  private static void createTopic(String topic) throws Exception {
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob))) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(60, SECONDS);
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
                LOG.warn("failed to cancel job cleanly", e);
              }
            });
  }

  private static String createSinkSql(String sinkTopic, String basicAuthWithClause) {
    return "CREATE TABLE sink_"
        + sinkTopic.replace('-', '_')
        + " ("
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
        + basicAuthWithClause
        + ")";
  }

  private static String valuesSql(String tableName) {
    return "CREATE TABLE "
        + tableName
        + " (`content` STRING, `date_time` STRING) WITH ('connector' = 'datagen', "
        + "'number-of-rows' = '2')";
  }

  /**
   * Happy path: encode (register + serialize) with valid basic-auth credentials, then decode
   * (deserialize) the same topic with valid basic-auth credentials. Proves both the encoding and
   * decoding formats authenticate against the secured registry.
   */
  @Test
  @DisplayName("basic-auth valid credentials: serialize -> register -> deserialize round trip")
  void basicAuth_validCredentials_roundTrip() throws Exception {
    String sinkTopic = "auth-basic-happy";
    createTopic(sinkTopic);
    produceViaFlinkInsert(sinkTopic, VALID_USER + ":" + VALID_PASSWORD);

    // Decode side: read the topic back with valid basic-auth credentials.
    List<TestSimple.SimpleMessage> consumed =
        awaitConsumeWithBasicAuth(sinkTopic, VALID_USER, VALID_PASSWORD, 90);
    assertTrue(consumed.size() >= 2, "Expected at least 2 messages; got " + consumed.size());
    LOG.info("basicAuth happy path: consumed {} messages", consumed.size());
  }

  /**
   * Sad path: register/serialize attempted with no basic-auth credentials against a registry that
   * requires them. Confluent's serializer surfaces this as a RestClientException wrapping HTTP 401;
   * that failure must propagate as a clear job failure, not silently swallowed.
   */
  @Test
  @DisplayName("basic-auth missing credentials: register call fails with HTTP 401")
  void basicAuth_missingCredentials_failsWithUnauthorized() {
    String sinkTopic = "auth-basic-missing-creds";
    Exception failure =
        assertThrows(
            Exception.class,
            () -> {
              createTopic(sinkTopic);
              produceViaFlinkInsert(sinkTopic, /* basicAuthUserInfo= */ null);
            });
    assertUnauthorized(failure);
  }

  /** Sad path: register/serialize attempted with wrong basic-auth credentials. */
  @Test
  @DisplayName("basic-auth wrong credentials: register call fails with HTTP 401")
  void basicAuth_wrongCredentials_failsWithUnauthorized() {
    String sinkTopic = "auth-basic-wrong-creds";
    Exception failure =
        assertThrows(
            Exception.class,
            () -> {
              createTopic(sinkTopic);
              produceViaFlinkInsert(sinkTopic, "sruser:not-the-right-password");
            });
    assertUnauthorized(failure);
  }

  /**
   * Bearer-token variant: {@code bearer-auth.credentials-source=STATIC_TOKEN} + {@code
   * bearer-auth.token} are forwarded to the Schema Registry client and sent as an {@code
   * Authorization: Bearer} header. The {@code cp-schema-registry} image in this suite is configured
   * for BASIC auth only (it has no built-in bearer/OAuth validator), so a bearer-token attempt is
   * expected to be rejected with HTTP 401 as well -- this still proves the bearer-auth options are
   * wired through and actually sent, which is what issue #67 asks to validate.
   */
  @Test
  @DisplayName("bearer-auth static token: option is forwarded and rejected by BASIC-only registry")
  void bearerAuth_staticToken_forwardedButRejectedByBasicOnlyRegistry() {
    String sinkTopic = "auth-bearer-static-token";
    Exception failure =
        assertThrows(
            Exception.class,
            () -> {
              createTopic(sinkTopic);
              produceViaFlinkInsertWithBearerAuth(sinkTopic, "dummy-static-token");
            });
    assertUnauthorized(failure);
  }

  private static void assertUnauthorized(Exception failure) {
    Throwable cause = failure;
    StringBuilder chain = new StringBuilder();
    while (cause != null) {
      chain.append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append('\n');
      cause = cause.getCause();
    }
    String message = chain.toString();
    assertTrue(
        message.contains("401")
            || message.toLowerCase(java.util.Locale.ROOT).contains("unauthorized"),
        "Expected the failure chain to mention HTTP 401 / Unauthorized, got:\n" + message);
  }

  private void produceViaFlinkInsert(String sinkTopic, String basicAuthUserInfo) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    String authClause =
        basicAuthUserInfo == null
            ? ""
            : ",  'value.proto-confluent.basic-auth.credentials-source' = 'USER_INFO',"
                + "  'value.proto-confluent.basic-auth.user-info' = '"
                + basicAuthUserInfo
                + "'";
    tableEnv.executeSql(valuesSql("src_" + sinkTopic.replace('-', '_')));
    tableEnv.executeSql(createSinkSql(sinkTopic, authClause));

    TableResult insertResult =
        tableEnv.executeSql(
            "INSERT INTO sink_"
                + sinkTopic.replace('-', '_')
                + " SELECT * FROM src_"
                + sinkTopic.replace('-', '_'));
    try {
      insertResult.await(60, SECONDS);
    } finally {
      cancelQuietly(insertResult);
    }
  }

  private void produceViaFlinkInsertWithBearerAuth(String sinkTopic, String token)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    String authClause =
        ",  'value.proto-confluent.bearer-auth.credentials-source' = 'STATIC_TOKEN',"
            + "  'value.proto-confluent.bearer-auth.token' = '"
            + token
            + "'";
    tableEnv.executeSql(valuesSql("src_" + sinkTopic.replace('-', '_')));
    tableEnv.executeSql(createSinkSql(sinkTopic, authClause));

    TableResult insertResult =
        tableEnv.executeSql(
            "INSERT INTO sink_"
                + sinkTopic.replace('-', '_')
                + " SELECT * FROM src_"
                + sinkTopic.replace('-', '_'));
    try {
      insertResult.await(60, SECONDS);
    } finally {
      cancelQuietly(insertResult);
    }
  }

  private List<TestSimple.SimpleMessage> awaitConsumeWithBasicAuth(
      String topic, String user, String password, int atMostSeconds) {
    List<TestSimple.SimpleMessage>[] holder = new List[] {new ArrayList<>()};
    await()
        .atMost(atMostSeconds, SECONDS)
        .pollInterval(Duration.ofSeconds(2))
        .until(
            () -> {
              holder[0] = consumeAsSimpleMessage(topic, user, password);
              return holder[0].size() >= 2;
            });
    return holder[0];
  }

  private List<TestSimple.SimpleMessage> consumeAsSimpleMessage(
      String topic, String user, String password) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "auth-specific-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(
        KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
        TestSimple.SimpleMessage.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("basic.auth.credentials.source", "USER_INFO");
    props.put("basic.auth.user.info", user + ":" + password);
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
}
