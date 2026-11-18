package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.cli.ProtoConfluentCli;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.OrderSchemas;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.ResolvedProtoSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistrySchemaResolver;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The {@code ddl} command against a real Schema Registry (issue #168): the statement it prints for
 * a registered subject, whose schema has a nested message, a repeated message, a map and a type
 * imported from a separately registered schema, is accepted by Flink and reads the topic. The same
 * command run as {@code java -jar <shadow JAR> ddl ...} prints the same statement.
 */
@DisplayName("ddl command line tool against a real Schema Registry")
class DdlCliIntegrationTest extends ProtoConfluentTestBase {

  private static final String TOPIC = "ddl-it-orders";
  private static final String SUBJECT = TOPIC + "-value";

  private static MiniClusterWithClientResource flinkCluster;
  private static String bootstrap;
  private static int orderSchemaId;

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

  /** Registers the schemas and produces two orders, once for the whole class. */
  private void registerAndProduceOnce() throws Exception {
    synchronized (DdlCliIntegrationTest.class) {
      if (orderSchemaId == 0) {
        orderSchemaId = OrderSchemas.register(getSchemaRegistryClient(), "ddl-it-money", SUBJECT);
        createTopic();
        produceOrders();
      }
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
    ResolvedProtoSchema resolved =
        SchemaRegistrySchemaResolver.create(getSchemaRegistryUrl(), Map.of()).resolve(SUBJECT);
    Descriptor order = resolved.getDescriptor();
    try (KafkaProducer<byte[], byte[]> producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName()))) {
      producer
          .send(new ProducerRecord<>(TOPIC, wireFormat(resolved.getId(), order(order, "o-1", 2))))
          .get(30, SECONDS);
      producer
          .send(new ProducerRecord<>(TOPIC, wireFormat(resolved.getId(), order(order, "o-2", 5))))
          .get(30, SECONDS);
    }
  }

  private static Message order(Descriptor order, String id, int quantity) {
    Descriptor customer = order.findNestedTypeByName("Customer");
    Descriptor item = order.findNestedTypeByName("LineItem");
    Descriptor money = order.findFieldByName("total").getMessageType();
    FieldDescriptor attributes = order.findFieldByName("attributes");
    Descriptor entry = attributes.getMessageType();
    DynamicMessage price =
        DynamicMessage.newBuilder(money)
            .setField(money.findFieldByName("currency"), "EUR")
            .setField(money.findFieldByName("units"), 10L)
            .build();
    return DynamicMessage.newBuilder(order)
        .setField(order.findFieldByName("order_id"), id)
        .setField(
            order.findFieldByName("customer"),
            DynamicMessage.newBuilder(customer)
                .setField(customer.findFieldByName("name"), "acme-" + id)
                .build())
        .addRepeatedField(
            order.findFieldByName("items"),
            DynamicMessage.newBuilder(item)
                .setField(item.findFieldByName("sku"), "sku-" + id)
                .setField(item.findFieldByName("quantity"), quantity)
                .setField(item.findFieldByName("price"), price)
                .build())
        .addRepeatedField(
            attributes,
            DynamicMessage.newBuilder(entry)
                .setField(entry.findFieldByName("key"), "channel")
                .setField(entry.findFieldByName("value"), "web")
                .build())
        .setField(
            order.findFieldByName("total"),
            price.toBuilder().setField(money.findFieldByName("units"), 10L * quantity).build())
        .addRepeatedField(order.findFieldByName("tags"), "first")
        .addRepeatedField(order.findFieldByName("tags"), "second")
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

  private String[] ddlArgs() {
    return new String[] {
      "ddl",
      "--url",
      getSchemaRegistryUrl(),
      "--subject",
      SUBJECT,
      "--bootstrap-servers",
      bootstrap,
      "--option",
      "properties.group.id=ddl-it-" + UUID.randomUUID(),
      "--option",
      "scan.startup.mode=earliest-offset",
      "--option",
      "scan.bounded.mode=latest-offset"
    };
  }

  private static String[] runInProcess(String... args) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    int status =
        new ProtoConfluentCli(
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8))
            .run(args);
    return new String[] {
      String.valueOf(status),
      out.toString(StandardCharsets.UTF_8),
      err.toString(StandardCharsets.UTF_8)
    };
  }

  @Test
  @DisplayName("the statement printed for a registered subject is accepted by Flink and reads it")
  void generatedStatementReadsTheTopic() throws Exception {
    registerAndProduceOnce();

    String[] result = runInProcess(ddlArgs());
    assertEquals("0", result[0], "stderr: " + result[2]);
    String statement = result[1].trim();
    assertTrue(statement.startsWith("CREATE TABLE `ddl-it-orders` (\n"), statement);
    assertTrue(statement.endsWith(";"), statement);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(statement.substring(0, statement.length() - 1));

    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> it =
        tableEnv
            .executeSql(
                "SELECT order_id, customer.name, items[1].sku, items[1].quantity,"
                    + " items[1].price.currency, attributes['channel'], total.units, tags"
                    + " FROM `ddl-it-orders`")
            .collect()) {
      it.forEachRemaining(rows::add);
    }
    rows.sort(Comparator.comparing(row -> String.valueOf(row.getField(0))));

    assertEquals(2, rows.size(), "both records are read: " + rows);
    assertEquals(
        Row.of("o-1", "acme-o-1", "sku-o-1", 2, "EUR", "web", 20L, new String[] {"first", "second"})
            .toString(),
        rows.get(0).toString());
    assertEquals("o-2", rows.get(1).getField(0));
    assertEquals(50L, rows.get(1).getField(6));
    assertTrue(Arrays.equals(new String[] {"first", "second"}, (String[]) rows.get(1).getField(7)));
  }

  @Test
  @DisplayName("a missing subject fails with exit status 1 and names the subject")
  void missingSubjectFails() {
    String[] result =
        runInProcess("ddl", "--url", getSchemaRegistryUrl(), "--subject", "nope-value");
    assertEquals("1", result[0]);
    assertTrue(result[2].contains("subject 'nope-value'"), result[2]);
  }

  @Test
  @DisplayName(
      "java -jar <shadow JAR> ddl prints the same statement, with nothing else on the classpath")
  void shadowJarPrintsTheSameStatement() throws Exception {
    registerAndProduceOnce();
    String[] args = ddlArgs();
    String[] inProcess = runInProcess(args);

    Process process = javaJar(args);
    assertEquals(0, process.waitFor(), stderrOf(process));
    assertEquals(inProcess[1], stdoutOf(process));
    assertEquals("", stderrOf(process), "no logging noise on stderr");
  }

  @Test
  @DisplayName("java -jar <shadow JAR> ddl --message-class loads the class from --classpath")
  void shadowJarLoadsAMessageClass() throws Exception {
    Process process =
        javaJar(
            "ddl",
            "--message-class",
            "com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage",
            "--classpath",
            System.getProperty("cli.testClasses"),
            "--columns-only");
    assertEquals(0, process.waitFor(), stderrOf(process));
    assertEquals("`content` STRING,\n`date_time` STRING\n", stdoutOf(process));
  }

  private static Process javaJar(String... args) throws Exception {
    String shadowJar = System.getProperty("cli.shadowJar");
    assertTrue(
        shadowJar != null && new File(shadowJar).isFile(),
        "the build passes the shadow JAR as cli.shadowJar, got " + shadowJar);
    Path java = Paths.get(System.getProperty("java.home"), "bin", "java");
    List<String> command = new ArrayList<>(List.of(java.toString(), "-jar", shadowJar));
    command.addAll(List.of(args));
    Path stdout = Files.createTempFile("ddl-cli-out", ".txt");
    Path stderr = Files.createTempFile("ddl-cli-err", ".txt");
    stdout.toFile().deleteOnExit();
    stderr.toFile().deleteOnExit();
    Process process =
        new ProcessBuilder(command)
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    OUTPUTS.put(process, new Path[] {stdout, stderr});
    return process;
  }

  private static final Map<Process, Path[]> OUTPUTS =
      new java.util.concurrent.ConcurrentHashMap<>();

  private static String stdoutOf(Process process) throws Exception {
    return Files.readString(OUTPUTS.get(process)[0]);
  }

  private static String stderrOf(Process process) throws Exception {
    return Files.readString(OUTPUTS.get(process)[1]);
  }
}
