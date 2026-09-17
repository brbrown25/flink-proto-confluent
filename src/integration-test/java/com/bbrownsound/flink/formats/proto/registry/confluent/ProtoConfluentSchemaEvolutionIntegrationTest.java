package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
 * Integration tests for schema evolution, compatibility enforcement, and subject naming against a
 * real Confluent Schema Registry. Covers GitHub issue #70:
 *
 * <ul>
 *   <li>records written under two different schema IDs (v1 and a v2 that adds an optional field)
 *       both deserialize through a table declared against v2, with the added column read as NULL
 *       for the v1 records;
 *   <li>{@code use-schema-id} pins the schema ID stamped into the Confluent wire header;
 *   <li>{@code auto-register-schemas=false} against an unregistered subject fails instead of
 *       silently registering;
 *   <li>{@code normalize-schemas} registers the normalized schema form, and a denormalized schema
 *       already stored in the registry is still compared verbatim;
 *   <li>the subject-naming strategy (TopicName vs RecordName) selects the registry subject;
 *   <li>an incompatible change with {@code auto-register-schemas=true} surfaces a clear error.
 * </ul>
 */
@Testcontainers
@DisplayName("Proto-confluent schema evolution, compatibility and subject-naming integration")
@Execution(ExecutionMode.SAME_THREAD)
class ProtoConfluentSchemaEvolutionIntegrationTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoConfluentSchemaEvolutionIntegrationTest.class);

  private static final String MESSAGE_NAME = "EvolvingMessage";
  private static final String EVOLUTION_TOPIC = "evo-it-source";

  private static final String V1_SCHEMA =
      "syntax = \"proto3\";\n"
          + "package com.bbrownsound.flink.formats.proto.evo;\n"
          + "\n"
          + "message EvolvingMessage {\n"
          + "  string content = 1;\n"
          + "  string date_time = 2;\n"
          + "}\n";

  /** v2 adds an optional field; backward compatible, so it registers as a second version. */
  private static final String V2_SCHEMA =
      "syntax = \"proto3\";\n"
          + "package com.bbrownsound.flink.formats.proto.evo;\n"
          + "\n"
          + "message EvolvingMessage {\n"
          + "  string content = 1;\n"
          + "  string date_time = 2;\n"
          + "  optional string extra = 3;\n"
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
  @DisplayName("records written under v1 and v2 schema IDs both deserialize through a v2 table")
  void crossVersionDeserialization_bothSchemaIdsReadByV2Table() throws Exception {
    createTopic(EVOLUTION_TOPIC);
    final String subject = EVOLUTION_TOPIC + "-value";

    final ProtobufSchema v1 = new ProtobufSchema(V1_SCHEMA);
    final ProtobufSchema v2 = new ProtobufSchema(V2_SCHEMA);
    final int v1Id = registryClient.register(subject, v1);
    final int v2Id = registryClient.register(subject, v2);
    assertNotEquals(v1Id, v2Id, "v2 must register as a distinct schema ID");
    assertEquals(2, registryClient.getAllVersions(subject).size(), "subject must hold 2 versions");

    final Descriptors.Descriptor v1Descriptor = v1.toDescriptor(MESSAGE_NAME);
    produce(
        EVOLUTION_TOPIC,
        DynamicMessage.newBuilder(v1Descriptor)
            .setField(v1Descriptor.findFieldByName("content"), "written-with-v1")
            .setField(v1Descriptor.findFieldByName("date_time"), "2025-01-01")
            .build());

    final Descriptors.Descriptor v2Descriptor = v2.toDescriptor(MESSAGE_NAME);
    produce(
        EVOLUTION_TOPIC,
        DynamicMessage.newBuilder(v2Descriptor)
            .setField(v2Descriptor.findFieldByName("content"), "written-with-v2")
            .setField(v2Descriptor.findFieldByName("date_time"), "2025-01-02")
            .setField(v2Descriptor.findFieldByName("extra"), "only-in-v2")
            .build());

    // The two records must actually carry different schema IDs on the wire, otherwise the test
    // would pass without ever exercising cross-version reads.
    final List<byte[]> raw = consume(EVOLUTION_TOPIC, 2);
    assertEquals(2, raw.size(), "expected both records on the source topic");
    assertEquals(v1Id, wireSchemaId(raw.get(0)));
    assertEquals(v2Id, wireSchemaId(raw.get(1)));

    // The table is declared against v2 (it has the `extra` column). The v1 record has no such
    // field in its writer schema, so that column must read as NULL rather than failing the job.
    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE evo_src ("
            + "  `content` STRING,"
            + "  `date_time` STRING,"
            + "  `extra` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + EVOLUTION_TOPIC
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'properties.group.id' = 'evo-it-"
            + UUID.randomUUID()
            + "',"
            + "  'scan.startup.mode' = 'earliest-offset',"
            + "  'scan.bounded.mode' = 'latest-offset',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + EVOLUTION_TOPIC
            + "',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")");

    final List<Row> rows = collect(tableEnv, "SELECT `content`, `date_time`, `extra` FROM evo_src");
    assertEquals(2, rows.size(), "both schema versions must deserialize; got " + rows);

    final Row v1Row = rowWithContent(rows, "written-with-v1");
    assertEquals("2025-01-01", String.valueOf(v1Row.getField(1)));
    assertNull(v1Row.getField(2), "a column absent from the v1 writer schema must read as NULL");

    final Row v2Row = rowWithContent(rows, "written-with-v2");
    assertEquals("2025-01-02", String.valueOf(v2Row.getField(1)));
    assertEquals("only-in-v2", String.valueOf(v2Row.getField(2)));
  }

  @Test
  @DisplayName("use-schema-id pins the schema ID stamped into the Confluent wire header")
  void useSchemaId_pinsWireSchemaIdOnSink() throws Exception {
    final String sinkTopic = "evo-it-use-schema-id";
    createTopic(sinkTopic);
    final String subject = sinkTopic + "-value";

    // Register the exact schema the sink will produce, then pin its ID explicitly.
    final ProtobufSchema pinned =
        new ProtobufSchema(TestSimple.SimpleMessage.getDescriptor()).copy();
    final int pinnedId = registryClient.register(subject, pinned);

    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE pinned_sink ("
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
            + "  'value.proto-confluent.auto-register-schemas' = 'false',"
            + "  'value.proto-confluent.use-schema-id' = '"
            + pinnedId
            + "',"
            + "  'value.proto-confluent.message-class' = "
            + "'com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")");

    tableEnv
        .executeSql("INSERT INTO pinned_sink VALUES ('pinned', '2025-02-01')")
        .await(120, SECONDS);

    final List<byte[]> written = consume(sinkTopic, 1);
    assertEquals(1, written.size(), "expected the pinned record on the sink topic");
    assertEquals(
        pinnedId,
        wireSchemaId(written.get(0)),
        "use-schema-id must control the schema ID in the wire header");
  }

  @Test
  @DisplayName("auto-register-schemas=false against an unregistered subject fails")
  void autoRegisterDisabled_unregisteredSubject_failsClearly() throws Exception {
    final String sinkTopic = "evo-it-no-auto-register";
    createTopic(sinkTopic);

    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE no_register_sink ("
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
            + "  'value.proto-confluent.auto-register-schemas' = 'false',"
            + "  'value.proto-confluent.message-class' = "
            + "'com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")");

    final Exception failure =
        assertThrows(
            Exception.class,
            () ->
                tableEnv
                    .executeSql("INSERT INTO no_register_sink VALUES ('nope', '2025-02-02')")
                    .await(120, SECONDS));
    assertTrue(
        registryClient.getAllSubjects().stream().noneMatch((sinkTopic + "-value")::equals),
        "the subject must not have been registered when auto-register-schemas=false");
    LOG.info("auto-register disabled failure: {}", rootMessage(failure));

    // Turning auto-registration on for the same subject registers it and the insert succeeds.
    tableEnv.executeSql(
        "CREATE TABLE auto_register_sink ("
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
            + "  'value.proto-confluent.message-class' = "
            + "'com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")");
    tableEnv
        .executeSql("INSERT INTO auto_register_sink VALUES ('yes', '2025-02-03')")
        .await(120, SECONDS);
    assertTrue(
        registryClient.getAllSubjects().contains(sinkTopic + "-value"),
        "auto-register-schemas=true must register the subject");
  }

  @Test
  @DisplayName("normalize-schemas registers the normalized form and does not rewrite history")
  void normalizeSchemas_registersNormalizedFormAgainstRegistry() throws Exception {
    // Every schema this format registers is derived from a Protobuf descriptor, and a
    // descriptor-derived canonical string is already in Confluent's normalized form (imports and
    // options sorted). The fixture below is the same schema with its imports swapped: equivalent,
    // but textually denormalized.
    final ProtobufSchema fromClass =
        new ProtobufSchema(TestRepeated.RepeatedTest.getDescriptor()).copy();
    final String normalizedForm = fromClass.normalize().canonicalString();
    final ProtobufSchema denormalized =
        new ProtobufSchema(swapFirstTwoImports(fromClass.canonicalString()));
    assertNotEquals(
        normalizedForm,
        denormalized.canonicalString(),
        "test fixture must actually be denormalized");
    assertEquals(
        normalizedForm,
        denormalized.normalize().canonicalString(),
        "test fixture must be equivalent to the schema the sink registers once normalized");

    // Both settings register the same, already-normalized schema exactly once.
    for (boolean normalize : new boolean[] {true, false}) {
      final String topic = "evo-it-normalize-" + (normalize ? "on" : "off");
      createTopic(topic);
      insertOneRepeatedRow(topic, normalize);
      final String subject = topic + "-value";
      assertEquals(
          1,
          registryClient.getAllVersions(subject).size(),
          "normalize-schemas=" + normalize + " must register exactly one version");
      assertEquals(
          normalizedForm,
          registryClient.getSchemaMetadata(subject, 1).getSchema(),
          "normalize-schemas=" + normalize + " must register the normalized schema form");
    }

    // A schema already stored in denormalized form is compared verbatim by the registry, so
    // normalize-schemas=true does not retroactively collapse it: the sink adds a second version.
    final String legacyTopic = "evo-it-normalize-legacy";
    createTopic(legacyTopic);
    final String legacySubject = legacyTopic + "-value";
    registryClient.register(legacySubject, denormalized);
    insertOneRepeatedRow(legacyTopic, true);
    assertEquals(
        2,
        registryClient.getAllVersions(legacySubject).size(),
        "a denormalized schema already in the registry is not matched by normalization; the "
            + "normalized form is registered as a new version");
    assertEquals(
        normalizedForm,
        registryClient.getSchemaMetadata(legacySubject, 2).getSchema(),
        "the new version must be the normalized form");
  }

  @Test
  @DisplayName("RecordNameStrategy registers under the record name instead of the topic name")
  void recordNameStrategy_registersUnderRecordNameSubject() throws Exception {
    final String sinkTopic = "evo-it-record-name-strategy";
    createTopic(sinkTopic);
    final String recordNameSubject = TestSimple.SimpleMessage.getDescriptor().getFullName();

    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE record_name_sink ("
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
            + "  'value.proto-confluent.message-class' = "
            + "'com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage',"
            + "  'value.proto-confluent.properties' = 'value.subject.name.strategy:"
            + "io.confluent.kafka.serializers.subject.RecordNameStrategy',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")");

    tableEnv
        .executeSql("INSERT INTO record_name_sink VALUES ('by-record-name', '2025-02-04')")
        .await(120, SECONDS);

    final List<String> subjects = new ArrayList<>(registryClient.getAllSubjects());
    assertTrue(
        subjects.contains(recordNameSubject),
        "RecordNameStrategy must register under '" + recordNameSubject + "'; got " + subjects);
    assertTrue(
        subjects.stream().noneMatch((sinkTopic + "-value")::equals),
        "RecordNameStrategy must not register under the TopicName subject; got " + subjects);
  }

  @Test
  @DisplayName("an incompatible change with auto-register-schemas=true surfaces a clear error")
  void incompatibleSchemaWithAutoRegister_failsWithClearError() throws Exception {
    final String sinkTopic = "evo-it-incompatible";
    createTopic(sinkTopic);
    final String subject = sinkTopic + "-value";

    // Register a first version whose field 1 is an int32. The sink pins SimpleMessage, whose
    // field 1 is a string, so auto-registering it is an incompatible change under BACKWARD.
    final String incompatibleBase =
        "syntax = \"proto3\";\n"
            + "package com.bbrownsound.flink.formats.proto.test.v1;\n"
            + "\n"
            + "message SimpleMessage {\n"
            + "  int32 content = 1;\n"
            + "  string date_time = 2;\n"
            + "}\n";
    registryClient.register(subject, new ProtobufSchema(incompatibleBase));
    registryClient.updateCompatibility(subject, "BACKWARD");

    final StreamTableEnvironment tableEnv = newTableEnv();
    tableEnv.executeSql(
        "CREATE TABLE incompatible_sink ("
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
            + "  'value.proto-confluent.message-class' = "
            + "'com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")");

    final Exception failure =
        assertThrows(
            Exception.class,
            () ->
                tableEnv
                    .executeSql("INSERT INTO incompatible_sink VALUES ('boom', '2025-02-05')")
                    .await(120, SECONDS));

    final String message = rootMessage(failure).toLowerCase(java.util.Locale.ROOT);
    LOG.info("incompatible registration failure: {}", message);
    assertTrue(
        message.contains("incompatible") || message.contains("409"),
        "the failure must name the compatibility violation; got: " + message);
    assertEquals(
        1,
        registryClient.getAllVersions(subject).size(),
        "the incompatible schema must not have been registered");
  }

  private void insertOneRepeatedRow(String topic, boolean normalize) throws Exception {
    final StreamTableEnvironment tableEnv = newTableEnv();
    final String table = "normalize_sink_" + (normalize ? "on" : "off");
    tableEnv.executeSql(
        "CREATE TABLE "
            + table
            + " ("
            + "  `values` ARRAY<STRING>"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = '"
            + topic
            + "',"
            + "  'properties.bootstrap.servers' = '"
            + bootstrapForJob
            + "',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = '"
            + schemaRegistryUrl
            + "',"
            + "  'value.proto-confluent.topic' = '"
            + topic
            + "',"
            + "  'value.proto-confluent.auto-register-schemas' = 'true',"
            + "  'value.proto-confluent.normalize-schemas' = '"
            + normalize
            + "',"
            + "  'value.proto-confluent.message-class' = "
            + "'com.bbrownsound.flink.formats.proto.test.v1.TestRepeated$RepeatedTest',"
            + "  'value.proto-confluent.is_key' = 'false'"
            + ")");
    tableEnv.executeSql("INSERT INTO " + table + " SELECT ARRAY['a', 'b']").await(120, SECONDS);
  }

  /**
   * Swaps the first two {@code import} lines of a canonical Protobuf schema so the result is
   * semantically identical but textually different from the normalized form.
   */
  private static String swapFirstTwoImports(String canonical) {
    final String[] lines = canonical.split("\n", -1);
    int first = -1;
    int second = -1;
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].startsWith("import ")) {
        if (first < 0) {
          first = i;
        } else {
          second = i;
          break;
        }
      }
    }
    if (first < 0 || second < 0) {
      throw new IllegalStateException(
          "fixture requires a schema with at least two imports:\n" + canonical);
    }
    final String tmp = lines[first];
    lines[first] = lines[second];
    lines[second] = tmp;
    return String.join("\n", lines);
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

  private static Row rowWithContent(List<Row> rows, String content) {
    return rows.stream()
        .filter(r -> content.equals(String.valueOf(r.getField(0))))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no row with content '" + content + "' in " + rows));
  }

  /** Reads the schema ID out of the Confluent wire header (magic byte + 4-byte big-endian ID). */
  private static int wireSchemaId(byte[] payload) throws Exception {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
      in.readByte();
      return in.readInt();
    }
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
    // The schemas are registered explicitly by the test so each record's writer schema (and thus
    // its wire schema ID) is deterministic.
    config.put("auto.register.schemas", "false");
    config.put("use.latest.version", "false");
    try (KafkaProducer<String, DynamicMessage> producer = new KafkaProducer<>(config)) {
      producer.send(new ProducerRecord<>(topic, message)).get(30, SECONDS);
      producer.flush();
    }
  }

  private static List<byte[]> consume(String topic, int expected) {
    final Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapForJob);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "evo-it-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    final List<byte[]> out = new ArrayList<>();
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
                        out.add(r.value());
                      }
                    });
                return out.size() >= expected;
              });
    }
    return out;
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
                  final Map<String, TopicDescription> desc =
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
}
