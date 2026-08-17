package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Covers GitHub issue #69: verifies that a record which fails to deserialize is actually produced
 * to the configured dead-letter topic, with the {@code error.class}, {@code error.message} and
 * {@code source.topic} headers set, using a real (Testcontainers) Kafka broker for the
 * dead-letter {@code KafkaProducer}.
 */
@Testcontainers
@DisplayName("ProtoRowDataDeserializationSchema dead-letter topic produce")
class ProtoRowDataDeserializationSchemaDeadLetterTest {

  private static final String DLQ_TOPIC = "dlq-deserialize-errors";
  private static final String SOURCE_TOPIC = "source-topic";

  @Container
  static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

  static String bootstrapServers;

  @BeforeAll
  static void setup() throws Exception {
    bootstrapServers = kafka.getBootstrapServers();
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
      admin.createTopics(List.of(new NewTopic(DLQ_TOPIC, 1, (short) 1))).all().get(60, SECONDS);
    }
  }

  @AfterAll
  static void teardown() {
    // Testcontainers lifecycle (@Container/@Testcontainers) handles stop/close.
  }

  private static DeserializationSchema.InitializationContext contextFor(MetricListener listener) {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return listener.getMetricGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(
            ProtoRowDataDeserializationSchemaDeadLetterTest.class.getClassLoader());
      }
    };
  }

  @Test
  void deserialize_producesPoisonRecordToDeadLetterTopicWithErrorHeaders() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());

    Configuration formatOptions = new Configuration();
    formatOptions.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    formatOptions.set(ProtoConfluentFormatOptions.TOPIC, SOURCE_TOPIC);
    formatOptions.set(ProtoConfluentFormatOptions.IS_KEY, false);
    formatOptions.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, "skip");
    formatOptions.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, DLQ_TOPIC);
    formatOptions.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
        Map.of("bootstrap.servers", bootstrapServers));

    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(formatOptions);

    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    schema.open(contextFor(listener));

    byte[] poison = new byte[] {0, 1, 2, 3, 4};
    assertNull(schema.deserialize(poison));

    List<ConsumerRecord<byte[], byte[]>> records = consumeDeadLetterTopic();
    assertTrue(records.size() >= 1, "Expected at least 1 record on the dead-letter topic");

    ConsumerRecord<byte[], byte[]> record = records.get(0);
    assertEquals(poison.length, record.value().length);
    for (int i = 0; i < poison.length; i++) {
      assertEquals(poison[i], record.value()[i]);
    }

    Header errorClass = record.headers().lastHeader("error.class");
    Header errorMessage = record.headers().lastHeader("error.message");
    Header sourceTopic = record.headers().lastHeader("source.topic");
    assertTrue(errorClass != null, "error.class header must be present");
    assertTrue(errorMessage != null, "error.message header must be present");
    assertTrue(sourceTopic != null, "source.topic header must be present");
    assertEquals(SOURCE_TOPIC, new String(sourceTopic.value(), StandardCharsets.UTF_8));
  }

  private List<ConsumerRecord<byte[], byte[]>> consumeDeadLetterTopic() {
    Map<String, Object> consumerProps = new java.util.HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    List<ConsumerRecord<byte[], byte[]>> out = new java.util.ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Collections.singletonList(DLQ_TOPIC));
      long deadline = System.currentTimeMillis() + 30_000;
      while (out.isEmpty() && System.currentTimeMillis() < deadline) {
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(2000));
        polled.forEach(out::add);
      }
    }
    return out;
  }
}
