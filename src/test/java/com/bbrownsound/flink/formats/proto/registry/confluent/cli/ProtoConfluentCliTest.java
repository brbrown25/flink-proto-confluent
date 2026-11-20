package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.OrderSchemas;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProtoConfluentCliTest {

  private static final String URL = "http://localhost:8081";

  @TempDir
  Path temp;

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private final List<String> requestedUrls = new ArrayList<>();
  private final List<Map<String, String>> requestedProperties = new ArrayList<>();
  private MockSchemaRegistryClient registry;
  private ProtoConfluentCli cli;

  @BeforeEach
  void setUp() throws Exception {
    registry = new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
    OrderSchemas.register(registry, "ddl-common-money", "orders-value");
    cli =
        new ProtoConfluentCli(
            new PrintStream(out, true, StandardCharsets.UTF_8),
            new PrintStream(err, true, StandardCharsets.UTF_8),
            (url, properties) -> {
              requestedUrls.add(url);
              requestedProperties.add(properties);
              return registry;
            });
  }

  private String out() {
    return out.toString(StandardCharsets.UTF_8);
  }

  private String err() {
    return err.toString(StandardCharsets.UTF_8);
  }

  @Test
  void printsACreateTableStatementForASubject() {
    assertEquals(0, cli.run("ddl", "--url", URL, "--subject", "orders-value"));

    assertEquals(
        String.join(
            "\n",
            "CREATE TABLE `orders` (",
            "  " + OrderSchemas.ORDER_COLUMNS.replace(",\n", ",\n  "),
            ") WITH (",
            "  'connector' = 'kafka',",
            "  'topic' = 'orders',",
            "  'value.format' = 'proto-confluent',",
            "  'value.proto-confluent.url' = 'http://localhost:8081',",
            "  'value.proto-confluent.topic' = 'orders'",
            ");",
            ""),
        out());
    assertEquals("", err());
    assertEquals(List.of(URL), requestedUrls);
    assertEquals(List.of(Map.of()), requestedProperties);
  }

  @Test
  void printsOnlyTheColumns() {
    assertEquals(0, cli.run("ddl", "--url", URL, "--subject", "orders-value", "--columns-only"));
    assertEquals(OrderSchemas.ORDER_COLUMNS + "\n", out());
  }

  @Test
  void appliesStrictNotNullAndKeepNullable() {
    assertEquals(
        0,
        cli.run(
            "ddl",
            "--url",
            URL,
            "--subject",
            "orders-value",
            "--columns-only",
            "--strict-not-null",
            "--keep-nullable",
            "order_id,items[].sku"));
    String columns = out();
    assertTrue(columns.startsWith("`order_id` STRING,\n"), columns);
    assertTrue(columns.contains("ROW<`sku` STRING, `quantity` INT NOT NULL"), columns);
    assertTrue(columns.contains("`customer` ROW<`name` STRING NOT NULL"), columns);
  }

  @Test
  void writesStatementOptionsAndConnectionOptions() {
    assertEquals(
        0,
        cli.run(
            "ddl",
            "--url",
            URL,
            "--subject",
            "orders-value",
            "--message-name",
            "ddl.orders.OrderKey",
            "--table",
            "order's",
            "--topic",
            "orders.v1",
            "--bootstrap-servers",
            "kafka:9092",
            "--option",
            "scan.startup.mode=earliest-offset",
            "--option",
            "connector=upsert-kafka",
            "--table-registry-url",
            "http://schema-registry:8081",
            "--basic-auth.credentials-source",
            "USER_INFO",
            "--basic-auth.user-info",
            "user:secret",
            "--property",
            "max.retries=3",
            "--property",
            "some.password=hidden"));

    assertEquals(
        String.join(
            "\n",
            "CREATE TABLE `order's` (",
            "  `order_id` STRING",
            ") WITH (",
            "  'connector' = 'upsert-kafka',",
            "  'topic' = 'orders.v1',",
            "  'properties.bootstrap.servers' = 'kafka:9092',",
            "  'value.format' = 'proto-confluent',",
            "  'value.proto-confluent.url' = 'http://schema-registry:8081',",
            "  'value.proto-confluent.topic' = 'orders.v1',",
            "  'value.proto-confluent.basic-auth.credentials-source' = 'USER_INFO',",
            "  'value.proto-confluent.properties.max.retries' = '3',",
            "  'scan.startup.mode' = 'earliest-offset'",
            ");",
            ""),
        out());
    assertTrue(
        err().contains("left basic-auth.user-info, properties.some.password out of the statement"),
        err());
    assertEquals(List.of(URL), requestedUrls, "the registry is read at --url");
    assertEquals(
        Map.of(
            "basic.auth.credentials.source", "USER_INFO",
            "basic.auth.user.info", "user:secret",
            "max.retries", "3",
            "some.password", "hidden"),
        requestedProperties.get(0));
  }

  @Test
  void includesSecretsOnRequest() {
    assertEquals(
        0,
        cli.run(
            "ddl",
            "--url",
            URL,
            "--subject",
            "orders-value",
            "--bearer-auth.token",
            "it's-secret",
            "--include-secrets"));
    assertTrue(out().contains("'value.proto-confluent.bearer-auth.token' = 'it''s-secret'"), out());
    assertEquals("", err());
  }

  @Test
  void readsACredentialFromAFile() throws Exception {
    Path token = Files.writeString(temp.resolve("token"), "from-file\n");
    assertEquals(
        0,
        cli.run(
            "ddl",
            "--url",
            URL,
            "--subject",
            "orders-value",
            "--bearer-auth.token-file",
            token.toString()));
    assertEquals(Map.of("bearer.auth.token", "from-file"), requestedProperties.get(0));
    assertFalse(out().contains("from-file"), out());
    assertTrue(err().contains("left bearer-auth.token out of the statement"), err());
  }

  @Test
  void printsTheStatementForAMessageClass() {
    assertEquals(
        0,
        cli.run(
            "ddl",
            "--message-class",
            "com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage",
            "--topic",
            "simple",
            "--url",
            URL));
    assertTrue(
        out().startsWith("CREATE TABLE `simple` (\n  `content` STRING,\n  `date_time` STRING\n)"),
        out());
    assertTrue(requestedUrls.isEmpty(), "a message class needs no registry");
  }

  @Test
  void loadsAMessageClassFromAClasspathEntry() throws Exception {
    String testClasses =
        java.nio.file.Paths.get(
                com.bbrownsound.flink.formats.proto.test.v1.TestSimple.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI())
            .toString();
    assertEquals(
        0,
        cli.run(
            "ddl",
            "--message-class",
            "com.bbrownsound.flink.formats.proto.test.v1.TestSimple$SimpleMessage",
            "--classpath",
            testClasses,
            "--columns-only"));
    assertEquals("`content` STRING,\n`date_time` STRING\n", out());
  }

  @Test
  void failsForAMissingClassOrClasspathEntry() {
    assertEquals(
        1,
        cli.run("ddl", "--message-class", "com.example.Missing", "--columns-only"));
    assertTrue(err().startsWith("error: Cannot load message class 'com.example.Missing'"), err());

    err.reset();
    assertEquals(
        1,
        cli.run(
            "ddl",
            "--message-class",
            "com.example.Missing",
            "--classpath",
            "/no/such/dir",
            "--columns-only"));
    assertEquals("error: --classpath entry does not exist: /no/such/dir\n", err());

    err.reset();
    assertEquals(1, cli.run("ddl", "--message-class", "java.lang.String", "--columns-only"));
    assertTrue(err().contains("Not a protobuf message class"), err());
  }

  @Test
  void failsForAMissingSubject() {
    assertEquals(1, cli.run("ddl", "--url", URL, "--subject", "nope-value"));
    assertTrue(err().startsWith("error: Cannot read subject 'nope-value'"), err());
    assertEquals("", out());
  }

  @Test
  void reportsUsageErrors() {
    assertEquals(2, cli.run("ddl", "--url", URL));
    assertTrue(err().startsWith("error: Pass either --subject"), err());
    assertTrue(err().contains(DdlArguments.USAGE), err());
    assertEquals("", out());
  }

  @Test
  void printsHelp() {
    assertEquals(0, cli.run("ddl", "--help"));
    assertEquals(DdlArguments.USAGE + "\n", out());

    out.reset();
    assertEquals(0, cli.run("--help"));
    assertEquals(ProtoConfluentCli.USAGE + "\n", out());
  }

  @Test
  void rejectsAMissingOrUnknownCommand() {
    assertEquals(2, cli.run());
    assertEquals(ProtoConfluentCli.USAGE + "\n", err());

    err.reset();
    assertEquals(2, cli.run("create"));
    assertTrue(err().startsWith("error: unknown command 'create'"), err());
    assertFalse(err().contains(DdlArguments.USAGE));
  }

  @Test
  void treatsCredentialPropertiesAsSecrets() {
    assertTrue(DdlCommand.isSecret("bearer-auth.token"));
    assertTrue(DdlCommand.isSecret("properties.basic.auth.user.info"));
    assertTrue(DdlCommand.isSecret("properties.sasl.jaas.config"));
    assertFalse(DdlCommand.isSecret("properties.max.retries"));
    assertFalse(DdlCommand.isSecret("ssl.truststore.location"));
  }

  private static final String TREE_SCHEMA =
      "syntax = \"proto3\";\npackage rec;\nmessage Tree {\n  string name = 1;\n  Tree child = 2;\n}";

  private int treeColumns(String... extra) throws Exception {
    registry.register("trees-value", new ProtobufSchema(TREE_SCHEMA));
    List<String> args =
        new ArrayList<>(List.of("ddl", "--url", URL, "--subject", "trees-value", "--columns-only"));
    args.addAll(List.of(extra));
    return cli.run(args.toArray(new String[0]));
  }

  @Test
  void failsOnARecursiveMessageByDefault() throws Exception {
    assertEquals(1, treeColumns());
    assertTrue(err().startsWith("error: Cyclic schemas are not supported."), err());
    assertTrue(err().contains("'recursive-message' option"), err());
    assertEquals("", out());
  }

  @Test
  void mapsARecursiveMessageAsBytes() throws Exception {
    assertEquals(0, treeColumns("--recursive-message", "bytes"));
    assertEquals("`name` STRING,\n`child` BYTES\n", out());
  }

  @Test
  void mapsARecursiveMessageAsJson() throws Exception {
    assertEquals(0, treeColumns("--recursive-message", "json"));
    assertEquals("`name` STRING,\n`child` STRING\n", out());
  }

  @Test
  void unrollsARecursiveMessageToADepth() throws Exception {
    assertEquals(0, treeColumns("--recursive-message", "row:2"));
    assertEquals(
        "`name` STRING,\n`child` ROW<`name` STRING, `child` ROW<`name` STRING>>\n", out());
  }

  @Test
  void rejectsAnInvalidRecursiveMessage() throws Exception {
    assertEquals(2, treeColumns("--recursive-message", "deep"));
    assertTrue(err().startsWith("error: 'recursive-message' must be"), err());
  }
}
