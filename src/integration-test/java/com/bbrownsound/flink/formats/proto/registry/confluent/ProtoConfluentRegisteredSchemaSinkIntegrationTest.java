package com.bbrownsound.flink.formats.proto.registry.confluent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.OrderSchemas;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Issue #210: a sink with no {@code message-class} and no {@code use-schema-id} writes with the
 * latest schema of its subject. When that schema has a {@code map} field (and here also imports a
 * type registered under another subject), the schema Confluent rebuilds from the descriptor renders
 * the map as a nested {@code map_entry} message, which does not match the registered text. The
 * sink must still write with the registered schema ID and register nothing, whether
 * auto-registration is off or on.
 */
@DisplayName("A sink writes with the registered schema of a subject that has a map and an import")
class ProtoConfluentRegisteredSchemaSinkIntegrationTest extends ProtoConfluentTestBase {

  private static MiniClusterWithClientResource flinkCluster;
  private static String bootstrap;

  @BeforeAll
  static void setUpCluster() throws Exception {
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
  static void tearDownCluster() {
    if (flinkCluster != null) {
      flinkCluster.after();
    }
  }

  @ParameterizedTest(name = "auto-register-schemas = {0}")
  @ValueSource(booleans = {false, true})
  void sinkWritesWithTheRegisteredSchemaId(boolean autoRegister) throws Exception {
    String topic = "registered-orders-" + autoRegister;
    String subject = topic + "-value";
    CachedSchemaRegistryClient registry = getSchemaRegistryClient();
    // The import is registered under its import path, the Confluent default reference subject.
    int schemaId = OrderSchemas.register(registry, OrderSchemas.MONEY_IMPORT, subject);
    Set<String> subjectsBefore = new HashSet<>(registry.getAllSubjects());
    List<Integer> moneyVersionsBefore = registry.getAllVersions(OrderSchemas.MONEY_IMPORT);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(
        "CREATE TABLE orders_sink (\n"
            + "  `order_id` STRING,\n"
            + "  `customer` ROW<`name` STRING, `email` STRING>,\n"
            + "  `items` ARRAY<ROW<`sku` STRING, `quantity` INT,"
            + " `price` ROW<`currency` STRING, `units` BIGINT>>>,\n"
            + "  `attributes` MAP<STRING, STRING>,\n"
            + "  `total` ROW<`currency` STRING, `units` BIGINT>,\n"
            + "  `tags` ARRAY<STRING>\n"
            + ") WITH (\n"
            + "  'connector' = 'kafka',\n"
            + "  'topic' = '"
            + topic
            + "',\n"
            + "  'properties.bootstrap.servers' = '"
            + bootstrap
            + "',\n"
            + "  'value.format' = 'proto-confluent',\n"
            + "  'value.proto-confluent.url' = '"
            + getSchemaRegistryUrl()
            + "',\n"
            + "  'value.proto-confluent.topic' = '"
            + topic
            + "',\n"
            + "  'value.proto-confluent.auto-register-schemas' = '"
            + autoRegister
            + "'\n"
            + ")");

    tableEnv
        .executeSql(
            "INSERT INTO orders_sink VALUES ('o-1', ROW('acme', 'a@example.com'),"
                + " ARRAY[ROW('sku-1', 2, ROW('EUR', CAST(5 AS BIGINT)))],"
                + " MAP['channel', 'web'], ROW('EUR', CAST(10 AS BIGINT)), ARRAY['t-1'])")
        .await(120, SECONDS);

    List<byte[]> values = consume(topic);
    assertEquals(1, values.size(), "one record");
    ByteBuffer wire = ByteBuffer.wrap(values.get(0));
    assertEquals(0, wire.get(), "magic byte");
    assertEquals(schemaId, wire.getInt(), "the record carries the registered schema ID");

    Descriptor order =
        ((ProtobufSchema)
                registry.parseSchema(new Schema(subject, registry.getLatestSchemaMetadata(subject)))
                    .orElseThrow())
            .toDescriptor();
    byte[] bytes = values.get(0);
    // Magic byte, schema ID, then the message index of the first message (a single 0 byte).
    assertEquals(0, bytes[5], "message index of ddl.orders.Order");
    DynamicMessage decoded =
        DynamicMessage.parseFrom(order, Arrays.copyOfRange(bytes, 6, bytes.length));
    assertEquals("o-1", decoded.getField(order.findFieldByName("order_id")));
    assertEquals(1, decoded.getRepeatedFieldCount(order.findFieldByName("attributes")));
    DynamicMessage total = (DynamicMessage) decoded.getField(order.findFieldByName("total"));
    assertEquals(10L, total.getField(total.getDescriptorForType().findFieldByName("units")));

    assertEquals(
        subjectsBefore, new HashSet<>(registry.getAllSubjects()), "the sink registers no subject");
    assertEquals(List.of(1), registry.getAllVersions(subject), "nor a new version");
    assertEquals(
        moneyVersionsBefore,
        registry.getAllVersions(OrderSchemas.MONEY_IMPORT),
        "nor a new version of the import");
  }

  private static List<byte[]> consume(String topic) {
    Map<String, Object> config =
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG, "registered-sink-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    List<byte[]> values = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config)) {
      consumer.subscribe(List.of(topic));
      await()
          .atMost(Duration.ofSeconds(60))
          .pollInterval(Duration.ofMillis(200))
          .until(() -> {
            for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
              values.add(record.value());
            }
            return !values.isEmpty();
          });
    }
    return values;
  }
}
