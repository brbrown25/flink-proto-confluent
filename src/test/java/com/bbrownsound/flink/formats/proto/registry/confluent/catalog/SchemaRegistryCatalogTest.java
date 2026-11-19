package com.bbrownsound.flink.formats.proto.registry.confluent.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.OrderSchemas;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class SchemaRegistryCatalogTest {

  private static final String URL = "http://registry:8081";
  private static final String KEY_SCHEMA =
      "syntax = \"proto3\";\npackage ddl.orders;\nmessage OrderKey {\n  string order_id = 1;\n}";
  private static final String PLAIN_SCHEMA =
      "syntax = \"proto3\";\npackage plain;\nmessage Event {\n  string name = 1;\n  int64 at = 2;\n}";
  private static final ObjectPath ORDERS = new ObjectPath("default", "orders");

  private MockSchemaRegistryClient client;
  private SchemaRegistryCatalog catalog;

  @BeforeEach
  void setUp() throws Exception {
    client = newClient();
    OrderSchemas.register(client, "ddl-common-money", "orders-value");
    client.register("orders-key", new ProtobufSchema(KEY_SCHEMA));
    client.register("events-value", new ProtobufSchema(PLAIN_SCHEMA));
    client.register(
        "avro-value", new AvroSchema("{\"type\":\"record\",\"name\":\"A\",\"fields\":[]}"));
    client.register("-value", new ProtobufSchema(PLAIN_SCHEMA));
    catalog = catalog(Map.of());
  }

  @AfterEach
  void tearDown() {
    catalog.close();
  }

  private static MockSchemaRegistryClient newClient() {
    return new MockSchemaRegistryClient(
        List.of(new ProtobufSchemaProvider(), new AvroSchemaProvider()));
  }

  private SchemaRegistryCatalog catalog(Map<String, String> extraOptions) {
    return catalog(extraOptions, client);
  }

  private static SchemaRegistryCatalog catalog(
      Map<String, String> extraOptions, SchemaRegistryClient registry) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("url", URL);
    options.putAll(extraOptions);
    SchemaRegistryCatalog c =
        new SchemaRegistryCatalog("sr", SchemaRegistryCatalogConfig.fromOptions(options), cfg -> registry);
    c.open();
    return c;
  }

  @Test
  void hasOneDatabase() throws Exception {
    assertEquals(List.of("default"), catalog.listDatabases());
    assertEquals("default", catalog.getDefaultDatabase());
    assertTrue(catalog.databaseExists("default"));
    assertFalse(catalog.databaseExists("other"));
    CatalogDatabase database = catalog.getDatabase("default");
    assertTrue(database.getProperties().isEmpty());
    assertTrue(database.getComment().contains("Schema Registry"));
    assertSame(database, database.copy());
    assertSame(database, database.copy(Map.of("a", "b")));
    assertEquals(database.getComment(), database.getDescription().orElseThrow());
    assertEquals(database.getComment(), database.getDetailedDescription().orElseThrow());
    assertThrows(DatabaseNotExistException.class, () -> catalog.getDatabase("other"));
    assertThrows(DatabaseNotExistException.class, () -> catalog.listTables("other"));
    assertThrows(DatabaseNotExistException.class, () -> catalog.listViews("other"));
    assertThrows(DatabaseNotExistException.class, () -> catalog.listFunctions("other"));
  }

  @Test
  void databaseNameIsConfigurable() throws Exception {
    SchemaRegistryCatalog custom = catalog(Map.of("default-database", "kafka"));
    assertEquals(List.of("kafka"), custom.listDatabases());
    assertEquals(List.of("events", "orders"), custom.listTables("kafka"));
    assertTrue(custom.tableExists(new ObjectPath("kafka", "orders")));
    assertFalse(custom.tableExists(ORDERS));
    assertThrows(TableNotExistException.class, () -> custom.getTable(ORDERS));
  }

  @Test
  void listsProtobufValueSubjectsAsTables() throws Exception {
    assertEquals(
        List.of("events", "orders"),
        catalog.listTables("default"),
        "avro-value, orders-key, the bare '-value' and the referenced money subject are skipped");
    assertTrue(catalog.listViews("default").isEmpty());
  }

  @Test
  void tableExistsOnlyForProtobufValueSubjects() {
    assertTrue(catalog.tableExists(ORDERS));
    assertTrue(catalog.tableExists(new ObjectPath("default", "events")));
    assertFalse(catalog.tableExists(new ObjectPath("default", "avro")), "not protobuf");
    assertFalse(catalog.tableExists(new ObjectPath("default", "nope")), "no subject");
    assertFalse(catalog.tableExists(new ObjectPath("other", "orders")), "no such database");
  }

  @Test
  void getTableHasKeyAndValueColumnsAndKafkaOptions() throws Exception {
    CatalogBaseTable table = catalog.getTable(ORDERS);
    assertInstanceOf(CatalogTable.class, table);
    assertEquals(CatalogBaseTable.TableKind.TABLE, table.getTableKind());

    assertEquals(
        List.of(
            "`key_order_id` STRING",
            "`order_id` STRING",
            "`customer` ROW<`name` STRING, `email` STRING>",
            "`items` ARRAY<ROW<`sku` STRING, `quantity` INT, `price` ROW<`currency` STRING, "
                + "`units` BIGINT>> NOT NULL> NOT NULL",
            "`attributes` MAP<STRING, STRING> NOT NULL",
            "`total` ROW<`currency` STRING, `units` BIGINT>",
            "`tags` ARRAY<STRING> NOT NULL"),
        columns(table.getUnresolvedSchema()));

    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("connector", "kafka");
    expected.put("topic", "orders");
    expected.put("key.format", "proto-confluent");
    expected.put("key.proto-confluent.url", URL);
    expected.put("key.proto-confluent.topic", "orders");
    expected.put(
        "key.proto-confluent.use-schema-id",
        String.valueOf(client.getLatestSchemaMetadata("orders-key").getId()));
    expected.put("key.proto-confluent.is_key", "true");
    expected.put("key.fields", "key_order_id");
    expected.put("key.fields-prefix", "key_");
    expected.put("value.fields-include", "EXCEPT_KEY");
    expected.put("value.format", "proto-confluent");
    expected.put("value.proto-confluent.url", URL);
    expected.put("value.proto-confluent.topic", "orders");
    expected.put(
        "value.proto-confluent.use-schema-id",
        String.valueOf(client.getLatestSchemaMetadata("orders-value").getId()));
    assertEquals(expected, table.getOptions());
    assertEquals(List.copyOf(expected.keySet()), List.copyOf(table.getOptions().keySet()));
    assertTrue(((CatalogTable) table).getPartitionKeys().isEmpty());

    String comment = table.getComment();
    assertTrue(comment.startsWith("Kafka topic 'orders'; value: ddl.orders.Order from subject 'orders-value' version 1 (schema ID "), comment);
    assertTrue(comment.contains("; key: ddl.orders.OrderKey from subject 'orders-key' version 1"), comment);
  }

  @Test
  void getTableWithoutAKeySubjectHasOnlyValueColumns() throws Exception {
    CatalogBaseTable table = catalog.getTable(new ObjectPath("default", "events"));
    assertEquals(List.of("`name` STRING", "`at` BIGINT"), columns(table.getUnresolvedSchema()));
    assertFalse(table.getOptions().containsKey("key.format"));
    assertFalse(table.getOptions().containsKey("value.fields-include"));
    assertFalse(table.getComment().contains("key:"));
  }

  @Test
  void aNonProtobufKeySubjectIsIgnored() throws Exception {
    client.register(
        "events-key", new AvroSchema("{\"type\":\"record\",\"name\":\"K\",\"fields\":[]}"));
    assertFalse(catalog.getTable(new ObjectPath("default", "events")).getOptions().containsKey("key.format"));
  }

  @Test
  void anEmptyKeyMessageAddsNoKeyColumns() throws Exception {
    client.register(
        "events-key", new ProtobufSchema("syntax = \"proto3\";\npackage k;\nmessage Empty {}"));
    CatalogBaseTable table = catalog.getTable(new ObjectPath("default", "events"));
    assertFalse(table.getOptions().containsKey("key.format"));
    assertFalse(table.getComment().contains("key:"));
  }

  @Test
  void keyColumnsCanBeTurnedOff() throws Exception {
    CatalogBaseTable table = catalog(Map.of("key.enabled", "false")).getTable(ORDERS);
    assertEquals("`order_id` STRING", columns(table.getUnresolvedSchema()).get(0));
    assertFalse(table.getOptions().containsKey("key.format"));
  }

  @Test
  void aKeyColumnThatClashesWithAValueColumnIsReported() {
    SchemaRegistryCatalog noPrefix = catalog(Map.of("key.fields-prefix", ""));
    CatalogException e = assertThrows(CatalogException.class, () -> noPrefix.getTable(ORDERS));
    assertTrue(e.getMessage().contains("key field 'order_id' of subject 'orders-key'"), e.getMessage());
    assertTrue(e.getMessage().contains("'key.fields-prefix'"), e.getMessage());
  }

  @Test
  void anEmptyKeyPrefixWithoutAClashIsNotWritten() throws Exception {
    client.register("events-key", new ProtobufSchema(KEY_SCHEMA));
    CatalogBaseTable table =
        catalog(Map.of("key.fields-prefix", "")).getTable(new ObjectPath("default", "events"));
    assertEquals("order_id", table.getOptions().get("key.fields"));
    assertFalse(table.getOptions().containsKey("key.fields-prefix"));
    assertEquals("EXCEPT_KEY", table.getOptions().get("value.fields-include"));
  }

  @Test
  void catalogOptionsReachEveryTable() throws Exception {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("properties.bootstrap.servers", "kafka:9092");
    options.put("properties.group.id", "g");
    options.put("scan.startup.mode", "earliest-offset");
    options.put("sink.delivery-guarantee", "at-least-once");
    options.put("basic-auth.credentials-source", "USER_INFO");
    options.put("basic-auth.user-info", "user:secret");
    options.put("schema-registry.properties.max.retries", "3");
    options.put("table.value.proto-confluent.on-deserialize-error", "skip");
    options.put("table.scan.startup.mode", "latest-offset");
    options.put("table.topic", "renamed");
    CatalogBaseTable table = catalog(options).getTable(new ObjectPath("default", "events"));

    Map<String, String> actual = table.getOptions();
    assertEquals("kafka:9092", actual.get("properties.bootstrap.servers"));
    assertEquals("g", actual.get("properties.group.id"));
    assertEquals("at-least-once", actual.get("sink.delivery-guarantee"));
    assertEquals("latest-offset", actual.get("scan.startup.mode"), "table.<option> wins");
    assertEquals("renamed", actual.get("topic"), "table.<option> may replace a generated option");
    assertEquals("skip", actual.get("value.proto-confluent.on-deserialize-error"));
    assertEquals("USER_INFO", actual.get("value.proto-confluent.basic-auth.credentials-source"));
    assertEquals("user:secret", actual.get("value.proto-confluent.basic-auth.user-info"));
    assertEquals("3", actual.get("value.proto-confluent.properties.max.retries"));
    assertFalse(actual.containsKey("schema-registry.properties.max.retries"));
    assertFalse(actual.containsKey("basic-auth.user-info"));
    assertFalse(table.getComment().contains("secret"));

    CatalogBaseTable keyed = catalog(options).getTable(ORDERS);
    assertEquals("user:secret", keyed.getOptions().get("key.proto-confluent.basic-auth.user-info"));
  }

  @Test
  void configMapsConnectionOptionsToClientProperties() {
    SchemaRegistryCatalogConfig config =
        SchemaRegistryCatalogConfig.fromOptions(
            Map.of(
                "url", URL,
                "bearer-auth.token", "t",
                "schema-registry.properties.max.retries", "3",
                "properties.bootstrap.servers", "kafka:9092",
                "unrelated", "x"));
    assertEquals(Map.of("bearer.auth.token", "t", "max.retries", "3"), config.getClientProperties());
    assertEquals(Map.of("properties.bootstrap.servers", "kafka:9092"), config.getTableOptions());
    assertEquals("default", config.getDefaultDatabase());
    assertTrue(config.isKeyEnabled());
    assertEquals("key_", config.getKeyFieldsPrefix());

    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class, () -> SchemaRegistryCatalogConfig.fromOptions(Map.of()));
    assertTrue(missing.getMessage().contains("'url'"), missing.getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> SchemaRegistryCatalogConfig.fromOptions(Map.of("url", " ")));
  }

  @Test
  void getTableOfAMissingOrNonProtobufSubjectIsTableNotExist() {
    assertThrows(TableNotExistException.class, () -> catalog.getTable(new ObjectPath("default", "avro")));
    assertThrows(TableNotExistException.class, () -> catalog.getTable(new ObjectPath("default", "nope")));
    assertThrows(TableNotExistException.class, () -> catalog.getTable(new ObjectPath("other", "orders")));
  }

  @Test
  void theLatestVersionIsResolvedOnEveryCall() throws Exception {
    assertEquals(2, columns(catalog.getTable(new ObjectPath("default", "events")).getUnresolvedSchema()).size());
    client.register(
        "events-value",
        new ProtobufSchema(PLAIN_SCHEMA.replace("int64 at = 2;", "int64 at = 2;\n  string source = 3;")));
    List<String> columns =
        columns(catalog.getTable(new ObjectPath("default", "events")).getUnresolvedSchema());
    assertEquals("`source` STRING", columns.get(2));
    assertEquals(
        String.valueOf(client.getLatestSchemaMetadata("events-value").getId()),
        catalog
            .getTable(new ObjectPath("default", "events"))
            .getOptions()
            .get("value.proto-confluent.use-schema-id"),
        "a sink writes with the version the columns came from");
  }

  @Test
  void everyMutationIsRejected() {
    CatalogPartitionSpec spec = new CatalogPartitionSpec(Map.of());
    List<Executable> mutations =
        List.of(
            () -> catalog.createDatabase("d", null, true),
            () -> catalog.dropDatabase("default", true, true),
            () -> catalog.alterDatabase("default", null, true),
            () -> catalog.createTable(ORDERS, null, true),
            () -> catalog.dropTable(ORDERS, true),
            () -> catalog.renameTable(ORDERS, "x", true),
            () -> catalog.alterTable(ORDERS, null, true),
            () -> catalog.createPartition(ORDERS, spec, null, true),
            () -> catalog.dropPartition(ORDERS, spec, true),
            () -> catalog.alterPartition(ORDERS, spec, null, true),
            () -> catalog.createFunction(ORDERS, null, true),
            () -> catalog.alterFunction(ORDERS, null, true),
            () -> catalog.dropFunction(ORDERS, true),
            () -> catalog.alterTableStatistics(ORDERS, null, true),
            () -> catalog.alterTableColumnStatistics(ORDERS, null, true),
            () -> catalog.alterPartitionStatistics(ORDERS, spec, null, true),
            () -> catalog.alterPartitionColumnStatistics(ORDERS, spec, null, true));
    for (Executable mutation : mutations) {
      UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, mutation);
      assertTrue(e.getMessage().startsWith("Catalog 'sr' is a read-only view of the Schema Registry; cannot "), e.getMessage());
    }
    UnsupportedOperationException create =
        assertThrows(UnsupportedOperationException.class, () -> catalog.createTable(ORDERS, null, false));
    assertTrue(create.getMessage().contains("register a protobuf schema under subject 'orders-value'"), create.getMessage());
  }

  @Test
  void hasNoPartitionsFunctionsOrStatistics() throws Exception {
    CatalogPartitionSpec spec = new CatalogPartitionSpec(Map.of());
    assertTrue(catalog.listPartitions(ORDERS).isEmpty());
    assertTrue(catalog.listPartitions(ORDERS, spec).isEmpty());
    assertTrue(catalog.listPartitionsByFilter(ORDERS, List.of()).isEmpty());
    assertFalse(catalog.partitionExists(ORDERS, spec));
    assertThrows(PartitionNotExistException.class, () -> catalog.getPartition(ORDERS, spec));
    assertTrue(catalog.listFunctions("default").isEmpty());
    assertFalse(catalog.functionExists(ORDERS));
    assertThrows(FunctionNotExistException.class, () -> catalog.getFunction(ORDERS));
    assertSame(CatalogTableStatistics.UNKNOWN, catalog.getTableStatistics(ORDERS));
    assertSame(CatalogColumnStatistics.UNKNOWN, catalog.getTableColumnStatistics(ORDERS));
    assertSame(CatalogTableStatistics.UNKNOWN, catalog.getPartitionStatistics(ORDERS, spec));
    assertSame(CatalogColumnStatistics.UNKNOWN, catalog.getPartitionColumnStatistics(ORDERS, spec));
  }

  @Test
  void openCreatesOneClientAndCloseClosesIt() {
    AtomicBoolean closed = new AtomicBoolean();
    int[] created = {0};
    MockSchemaRegistryClient closing =
        new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider())) {
          @Override
          public void close() {
            closed.set(true);
          }
        };
    SchemaRegistryCatalog c =
        new SchemaRegistryCatalog(
            "sr",
            SchemaRegistryCatalogConfig.fromOptions(Map.of("url", URL)),
            cfg -> {
              created[0]++;
              return closing;
            });
    CatalogException notOpen = assertThrows(CatalogException.class, () -> c.listTables("default"));
    assertEquals("Catalog 'sr' is not open.", notOpen.getMessage());
    c.open();
    c.open();
    assertEquals(1, created[0]);
    c.close();
    assertTrue(closed.get());
    c.close();
  }

  @Test
  void aFailingCloseIsACatalogException() {
    MockSchemaRegistryClient failing =
        new MockSchemaRegistryClient() {
          @Override
          public void close() throws IOException {
            throw new IOException("boom");
          }
        };
    SchemaRegistryCatalog c = catalog(Map.of(), failing);
    assertThrows(CatalogException.class, c::close);
  }

  @Test
  void theDefaultConstructorBuildsARegistryClientFromTheConfig() {
    SchemaRegistryCatalog c =
        new SchemaRegistryCatalog(
            "sr", SchemaRegistryCatalogConfig.fromOptions(Map.of("url", "http://localhost:1")));
    c.open();
    try {
      CatalogException e = assertThrows(CatalogException.class, () -> c.listTables("default"));
      assertTrue(e.getMessage().startsWith("Cannot list subjects in the Schema Registry at http://localhost:1"), e.getMessage());
      CatalogException exists = assertThrows(CatalogException.class, () -> c.tableExists(ORDERS));
      assertTrue(exists.getMessage().startsWith("Cannot reach the Schema Registry to read subject 'orders-value'"), exists.getMessage());
    } finally {
      c.close();
    }
  }

  @Test
  void registryErrorsOtherThanNotFoundAreCatalogExceptions() throws Exception {
    MockSchemaRegistryClient failing =
        new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider())) {
          @Override
          public SchemaMetadata getLatestSchemaMetadata(String subject)
              throws RestClientException {
            throw new RestClientException("unauthorized", 401, 40101);
          }

          @Override
          public Collection<String> getAllSubjects() {
            return List.of("orders-value");
          }
        };
    SchemaRegistryCatalog c = catalog(Map.of(), failing);
    CatalogException e = assertThrows(CatalogException.class, () -> c.listTables("default"));
    assertTrue(e.getMessage().startsWith("Cannot read subject 'orders-value' from the Schema Registry"), e.getMessage());
    assertThrows(CatalogException.class, () -> c.getTable(ORDERS));
  }

  @Test
  void aSubjectWithoutVersionsIsNotATable() throws Exception {
    MockSchemaRegistryClient noVersions =
        new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider())) {
          @Override
          public SchemaMetadata getLatestSchemaMetadata(String subject)
              throws RestClientException {
            throw new RestClientException("no versions", 404, 40402);
          }

          @Override
          public Collection<String> getAllSubjects() {
            return List.of("orders-value");
          }
        };
    SchemaRegistryCatalog c = catalog(Map.of(), noVersions);
    assertTrue(c.listTables("default").isEmpty());
    assertFalse(c.tableExists(ORDERS));
    assertThrows(TableNotExistException.class, () -> c.getTable(ORDERS));
  }

  @Test
  void aSchemaThatDisappearsWhileItIsReadIsTableNotExist() throws Exception {
    Map<String, Integer> failures = new HashMap<>();
    MockSchemaRegistryClient racing =
        new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider())) {
          @Override
          public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
              throws IOException, RestClientException {
            Integer code = failures.get(subject);
            if (code != null) {
              throw new RestClientException("gone", code / 100, code);
            }
            return super.getSchemaBySubjectAndId(subject, id);
          }
        };
    racing.register("events-value", new ProtobufSchema(PLAIN_SCHEMA));
    SchemaRegistryCatalog c = catalog(Map.of(), racing);

    failures.put("events-value", 40402);
    assertThrows(TableNotExistException.class, () -> c.getTable(new ObjectPath("default", "events")));

    failures.put("events-value", 50001);
    CatalogException e =
        assertThrows(CatalogException.class, () -> c.getTable(new ObjectPath("default", "events")));
    assertTrue(e.getMessage().startsWith("Cannot read table default.events: "), e.getMessage());
  }

  // ---- recursive messages (issue #216) ------------------------------------------------------

  private static final String TREE_SCHEMA =
      "syntax = \"proto3\";\npackage rec;\nmessage Tree {\n  string name = 1;\n  Tree child = 2;\n}";
  private static final ObjectPath TREES = new ObjectPath("default", "trees");

  private List<String> treeColumns(String recursion) throws Exception {
    client.register("trees-value", new ProtobufSchema(TREE_SCHEMA));
    client.register("trees-key", new ProtobufSchema(TREE_SCHEMA));
    SchemaRegistryCatalog c = catalog(Map.of("recursive-message", recursion));
    try {
      return columns(c.getTable(TREES).getUnresolvedSchema());
    } finally {
      c.close();
    }
  }

  @Test
  void aRecursiveMessageFailsTheTableByDefault() throws Exception {
    client.register("trees-value", new ProtobufSchema(TREE_SCHEMA));
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> catalog.getTable(TREES));
    assertTrue(e.getMessage().startsWith("Cyclic schemas are not supported."), e.getMessage());
    assertTrue(e.getMessage().contains("'recursive-message' option"), e.getMessage());
    assertEquals(List.of("events", "orders", "trees"), catalog.listTables("default"));
  }

  @Test
  void recursiveMessageMapsKeyAndValueColumnsAsBytes() throws Exception {
    assertEquals(
        List.of(
            "`key_name` STRING",
            "`key_child` BYTES",
            "`name` STRING",
            "`child` BYTES"),
        treeColumns("bytes"));
  }

  @Test
  void recursiveMessageMapsColumnsAsJson() throws Exception {
    assertEquals(
        List.of("`key_name` STRING", "`key_child` STRING", "`name` STRING", "`child` STRING"),
        treeColumns("json"));
  }

  @Test
  void recursiveMessageUnrollsColumnsToADepth() throws Exception {
    assertEquals(
        List.of(
            "`key_name` STRING",
            "`key_child` ROW<`name` STRING>",
            "`name` STRING",
            "`child` ROW<`name` STRING>"),
        treeColumns("row:1"));
  }

  private static List<String> columns(Schema schema) {
    return schema.getColumns().stream()
        .map(
            column -> {
              Schema.UnresolvedPhysicalColumn physical = (Schema.UnresolvedPhysicalColumn) column;
              return "`" + physical.getName() + "` " + physical.getDataType().toString();
            })
        .collect(Collectors.toList());
  }
}
