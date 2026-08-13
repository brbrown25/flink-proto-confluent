package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
 * Integration test: proves the {@code ssl.keystore.*} / {@code ssl.truststore.*} proto-confluent
 * format options (wired in {@code RegistryProtoFormatFactory.buildOptionalPropertiesMap}) actually
 * establish TLS / mutual TLS against an HTTPS Schema Registry. Covers GitHub issue #68.
 *
 * <p>TLS material (server keystore, client keystore, and matching truststores) is generated once at
 * class-load time via the JDK {@code keytool} binary into a temp directory -- nothing is committed
 * to the repo. Certificate generation happens in a static field initializer so it completes before
 * the {@code @Container} fields (also static) are initialized, since Testcontainers starts
 * {@code @Container} fields before any {@code @BeforeAll} method runs.
 */
@Testcontainers
@DisplayName("Proto-confluent TLS / mTLS Schema Registry integration")
@Execution(ExecutionMode.SAME_THREAD)
class ProtoConfluentTlsSchemaRegistryIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoConfluentTlsSchemaRegistryIntegrationTest.class);

  private static final String STORE_PASSWORD = "changeit";
  private static final Network network = Network.newNetwork();
  private static final TlsMaterial TLS = TlsMaterial.generate();

  @Container
  static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
          .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
          .withNetwork(network)
          .withExposedPorts(9093)
          .withNetworkAliases("kafka");

  /** HTTPS registry, no client-auth required: exercises one-way TLS (server auth only). */
  @Container
  static GenericContainer<?> schemaRegistryTls =
      new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
          .withNetwork(network)
          .withExposedPorts(8081)
          .withCopyFileToContainer(
              MountableFile.forHostPath(TLS.serverKeystore),
              "/etc/schema-registry/tls/server.keystore.jks")
          .withCopyFileToContainer(
              MountableFile.forHostPath(TLS.clientTruststore),
              "/etc/schema-registry/tls/self.truststore.jks")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas_tls")
          .withEnv("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID", "schema-registry-tls")
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry-tls")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "https://0.0.0.0:8081")
          .withEnv("SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL", "https")
          .withEnv(
              "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION",
              "/etc/schema-registry/tls/server.keystore.jks")
          .withEnv("SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD", STORE_PASSWORD)
          .withEnv("SCHEMA_REGISTRY_SSL_KEY_PASSWORD", STORE_PASSWORD)
          // Trusts its own server cert so inter-instance leader-forwarding calls (which this
          // single-node registry makes to itself) succeed over HTTPS.
          .withEnv(
              "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION",
              "/etc/schema-registry/tls/self.truststore.jks")
          .withEnv("SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD", STORE_PASSWORD)
          .withEnv("SCHEMA_REGISTRY_SSL_CLIENT_AUTH", "false")
          .dependsOn(kafka)
          .waitingFor(Wait.forLogMessage(".*Server started.*\\n", 1));

  /** HTTPS registry with client-auth required: exercises mutual TLS. */
  @Container
  static GenericContainer<?> schemaRegistryMtls =
      new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
          .withNetwork(network)
          .withExposedPorts(8081)
          .withCopyFileToContainer(
              MountableFile.forHostPath(TLS.serverKeystore),
              "/etc/schema-registry/tls/server.keystore.jks")
          .withCopyFileToContainer(
              MountableFile.forHostPath(TLS.serverTruststore),
              "/etc/schema-registry/tls/server.truststore.jks")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas_mtls")
          .withEnv("SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID", "schema-registry-mtls")
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry-mtls")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "https://0.0.0.0:8081")
          .withEnv("SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL", "https")
          .withEnv(
              "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION",
              "/etc/schema-registry/tls/server.keystore.jks")
          .withEnv("SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD", STORE_PASSWORD)
          .withEnv("SCHEMA_REGISTRY_SSL_KEY_PASSWORD", STORE_PASSWORD)
          .withEnv(
              "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION",
              "/etc/schema-registry/tls/server.truststore.jks")
          .withEnv("SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD", STORE_PASSWORD)
          .withEnv("SCHEMA_REGISTRY_SSL_CLIENT_AUTH", "true")
          .dependsOn(kafka)
          .waitingFor(Wait.forLogMessage(".*Server started.*\\n", 1));

  static MiniClusterWithClientResource flinkCluster;
  static String bootstrapForJob;
  static String tlsRegistryUrl;
  static String mtlsRegistryUrl;

  @BeforeAll
  static void setup() throws Exception {
    String bootstrapServers =
        String.format("PLAINTEXT://%s:%s", kafka.getHost(), kafka.getMappedPort(9093));
    bootstrapForJob = bootstrapServers.substring("PLAINTEXT://".length());
    tlsRegistryUrl =
        "https://" + schemaRegistryTls.getHost() + ":" + schemaRegistryTls.getMappedPort(8081);
    mtlsRegistryUrl =
        "https://" + schemaRegistryMtls.getHost() + ":" + schemaRegistryMtls.getMappedPort(8081);

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
    TLS.cleanup();
  }

  private static void createTopic(String bootstrap, String topic) throws Exception {
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
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

  private static String createSinkSql(String registryUrl, String sinkTopic, String sslWithClause) {
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
        + registryUrl
        + "',"
        + "  'value.proto-confluent.topic' = '"
        + sinkTopic
        + "',"
        + "  'value.proto-confluent.auto-register-schemas' = 'true',"
        + "  'value.proto-confluent.is_key' = 'false'"
        + sslWithClause
        + ")";
  }

  private static String valuesSql(String tableName) {
    return "CREATE TABLE "
        + tableName
        + " (`content` STRING, `date_time` STRING) WITH ('connector' = 'datagen', "
        + "'number-of-rows' = '2')";
  }

  /**
   * Happy path: {@code ssl.truststore.*} trusts the registry's self-signed cert -- serialize
   * (register) and deserialize succeed over one-way TLS.
   */
  @Test
  @DisplayName("TLS with trusted truststore: serialize -> register -> deserialize round trip")
  void tls_trustedTruststore_roundTrip() throws Exception {
    String sinkTopic = "tls-happy";
    createTopic(bootstrapForJob, sinkTopic);
    String sslClause =
        ",  'value.proto-confluent.ssl.truststore.location' = '"
            + TLS.clientTruststore
            + "',"
            + "  'value.proto-confluent.ssl.truststore.password' = '"
            + STORE_PASSWORD
            + "'";
    produceViaFlinkInsert(tlsRegistryUrl, sinkTopic, sslClause);

    List<TestSimple.SimpleMessage> consumed =
        awaitConsumeOverTls(tlsRegistryUrl, sinkTopic, TLS.clientTruststore, null, 90);
    assertTrue(consumed.size() >= 2, "Expected at least 2 messages; got " + consumed.size());
    LOG.info("TLS happy path: consumed {} messages", consumed.size());
  }

  /**
   * Sad path: no {@code ssl.truststore.*} option set, so the JVM default trust store is used, which
   * does not trust the registry's self-signed cert. The TLS handshake must fail clearly.
   */
  @Test
  @DisplayName("TLS with untrusted cert (no truststore): handshake fails clearly")
  void tls_untrustedCert_handshakeFails() {
    String sinkTopic = "tls-untrusted";
    Exception failure =
        assertThrows(
            Exception.class,
            () -> {
              createTopic(bootstrapForJob, sinkTopic);
              produceViaFlinkInsert(tlsRegistryUrl, sinkTopic, /* sslClause= */ "");
            });
    assertTlsFailure(failure);
  }

  /**
   * mTLS: server requires client-auth. {@code ssl.keystore.*} presents the client cert and {@code
   * ssl.truststore.*} trusts the server cert -- round trip succeeds over mutual TLS.
   */
  @Test
  @DisplayName(
      "mTLS with client keystore + truststore: serialize -> register -> deserialize round trip")
  void mtls_clientKeystoreAndTruststore_roundTrip() throws Exception {
    String sinkTopic = "mtls-happy";
    createTopic(bootstrapForJob, sinkTopic);
    String sslClause =
        ",  'value.proto-confluent.ssl.truststore.location' = '"
            + TLS.clientTruststore
            + "',"
            + "  'value.proto-confluent.ssl.truststore.password' = '"
            + STORE_PASSWORD
            + "',"
            + "  'value.proto-confluent.ssl.keystore.location' = '"
            + TLS.clientKeystore
            + "',"
            + "  'value.proto-confluent.ssl.keystore.password' = '"
            + STORE_PASSWORD
            + "'";
    produceViaFlinkInsert(mtlsRegistryUrl, sinkTopic, sslClause);

    List<TestSimple.SimpleMessage> consumed =
        awaitConsumeOverTls(
            mtlsRegistryUrl, sinkTopic, TLS.clientTruststore, TLS.clientKeystore, 90);
    assertTrue(consumed.size() >= 2, "Expected at least 2 messages; got " + consumed.size());
    LOG.info("mTLS happy path: consumed {} messages", consumed.size());
  }

  private static void assertTlsFailure(Exception failure) {
    Throwable cause = failure;
    StringBuilder chain = new StringBuilder();
    while (cause != null) {
      chain.append(cause.getClass().getName()).append(": ").append(cause.getMessage()).append('\n');
      cause = cause.getCause();
    }
    String message = chain.toString().toLowerCase(Locale.ROOT);
    assertTrue(
        message.contains("ssl")
            || message.contains("pkix")
            || message.contains("certificate")
            || message.contains("handshake"),
        "Expected the failure chain to mention an SSL/handshake/certificate failure, got:\n"
            + chain);
  }

  private void produceViaFlinkInsert(String registryUrl, String sinkTopic, String sslClause)
      throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    tableEnv.executeSql(valuesSql("src_" + sinkTopic.replace('-', '_')));
    tableEnv.executeSql(createSinkSql(registryUrl, sinkTopic, sslClause));

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

  private List<TestSimple.SimpleMessage> awaitConsumeOverTls(
      String registryUrl, String topic, Path truststore, Path keystore, int atMostSeconds) {
    List<TestSimple.SimpleMessage>[] holder = new List[] {new ArrayList<>()};
    await()
        .atMost(atMostSeconds, SECONDS)
        .pollInterval(Duration.ofSeconds(2))
        .until(
            () -> {
              holder[0] = consumeAsSimpleMessage(registryUrl, topic, truststore, keystore);
              return holder[0].size() >= 2;
            });
    return holder[0];
  }

  private List<TestSimple.SimpleMessage> consumeAsSimpleMessage(
      String registryUrl, String topic, Path truststore, Path keystore) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "tls-specific-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
    props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
    props.put(
        KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE,
        TestSimple.SimpleMessage.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put("schema.registry.ssl.truststore.location", truststore.toString());
    props.put("schema.registry.ssl.truststore.password", STORE_PASSWORD);
    if (keystore != null) {
      props.put("schema.registry.ssl.keystore.location", keystore.toString());
      props.put("schema.registry.ssl.keystore.password", STORE_PASSWORD);
    }
    List<TestSimple.SimpleMessage> out = new ArrayList<>();
    try (KafkaConsumer<String, TestSimple.SimpleMessage> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      ConsumerRecords<String, TestSimple.SimpleMessage> records =
          consumer.poll(Duration.ofMillis(5000));
      records.forEach(
          r -> {
            if (r.value() != null) {
              out.add(r.value());
            }
          });
    }
    return out;
  }

  /**
   * Generates a self-signed server cert/keystore, a self-signed client cert/keystore, a client
   * truststore trusting the server cert, and a server truststore trusting the client cert -- all
   * via {@code keytool}, in a scratch temp directory cleaned up in {@link #cleanup()}.
   */
  private static final class TlsMaterial {
    final Path dir;
    final Path serverKeystore;
    final Path clientKeystore;
    final Path clientTruststore;
    final Path serverTruststore;

    private TlsMaterial(
        Path dir,
        Path serverKeystore,
        Path clientKeystore,
        Path clientTruststore,
        Path serverTruststore) {
      this.dir = dir;
      this.serverKeystore = serverKeystore;
      this.clientKeystore = clientKeystore;
      this.clientTruststore = clientTruststore;
      this.serverTruststore = serverTruststore;
    }

    static TlsMaterial generate() {
      try {
        Path dir = Files.createTempDirectory("proto-confluent-tls-it-");
        Path serverKeystore = dir.resolve("server.keystore.jks");
        Path clientKeystore = dir.resolve("client.keystore.jks");
        Path clientTruststore = dir.resolve("client.truststore.jks");
        Path serverTruststore = dir.resolve("server.truststore.jks");
        Path serverCert = dir.resolve("server.cert");
        Path clientCert = dir.resolve("client.cert");

        keytool(
            "-genkeypair",
            "-alias",
            "server",
            "-keystore",
            serverKeystore.toString(),
            "-storepass",
            STORE_PASSWORD,
            "-keypass",
            STORE_PASSWORD,
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-validity",
            "3650",
            "-dname",
            "CN=schema-registry-tls, OU=test, O=flink-proto-confluent",
            "-ext",
            "SAN=dns:schema-registry-tls,dns:schema-registry-mtls,dns:localhost,ip:127.0.0.1");

        keytool(
            "-genkeypair",
            "-alias",
            "client",
            "-keystore",
            clientKeystore.toString(),
            "-storepass",
            STORE_PASSWORD,
            "-keypass",
            STORE_PASSWORD,
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-validity",
            "3650",
            "-dname",
            "CN=flink-client, OU=test, O=flink-proto-confluent");

        keytool(
            "-exportcert",
            "-alias",
            "server",
            "-keystore",
            serverKeystore.toString(),
            "-storepass",
            STORE_PASSWORD,
            "-file",
            serverCert.toString(),
            "-rfc");

        keytool(
            "-exportcert",
            "-alias",
            "client",
            "-keystore",
            clientKeystore.toString(),
            "-storepass",
            STORE_PASSWORD,
            "-file",
            clientCert.toString(),
            "-rfc");

        keytool(
            "-importcert",
            "-alias",
            "server",
            "-keystore",
            clientTruststore.toString(),
            "-storepass",
            STORE_PASSWORD,
            "-file",
            serverCert.toString(),
            "-noprompt");

        keytool(
            "-importcert",
            "-alias",
            "client",
            "-keystore",
            serverTruststore.toString(),
            "-storepass",
            STORE_PASSWORD,
            "-file",
            clientCert.toString(),
            "-noprompt");
        keytool(
            "-importcert",
            "-alias",
            "server",
            "-keystore",
            serverTruststore.toString(),
            "-storepass",
            STORE_PASSWORD,
            "-file",
            serverCert.toString(),
            "-noprompt");

        return new TlsMaterial(
            dir, serverKeystore, clientKeystore, clientTruststore, serverTruststore);
      } catch (IOException e) {
        throw new IllegalStateException("failed to generate TLS test material", e);
      }
    }

    void cleanup() {
      try (var stream = Files.walk(dir)) {
        stream
            .sorted(java.util.Comparator.reverseOrder())
            .forEach(
                p -> {
                  try {
                    Files.deleteIfExists(p);
                  } catch (IOException ignored) {
                    // best effort
                  }
                });
      } catch (IOException ignored) {
        // best effort
      }
    }

    private static void keytool(String... args) throws IOException {
      String keytoolBin = System.getProperty("java.home") + "/bin/keytool";
      List<String> command = new ArrayList<>();
      command.add(keytoolBin);
      Collections.addAll(command, args);
      try {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output =
            new String(
                process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        int exit = process.waitFor();
        if (exit != 0) {
          throw new IOException("keytool " + args[0] + " failed (exit " + exit + "):\n" + output);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("keytool invocation interrupted", e);
      }
    }
  }
}
