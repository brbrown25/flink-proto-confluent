package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers GitHub issue #71: negative-path behavior of the error-handling options. Asserts the
 * (previously silent) contract that a {@code dead-letter-topic} without {@code bootstrap.servers}
 * creates no producer, and that an unrecognized {@code on-deserialize-error} value degrades to the
 * {@code skip} path rather than failing.
 */
@DisplayName("ProtoRowDataDeserializationSchema error-handling negative paths")
class ProtoRowDataDeserializationSchemaNegativeConfigTest {

  private static final byte[] POISON = new byte[] {0, 1, 2, 3, 4};

  private static RowType rowType() {
    return (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
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
            ProtoRowDataDeserializationSchemaNegativeConfigTest.class.getClassLoader());
      }
    };
  }

  private static Configuration baseOptions(String onDeserializeError) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "source-topic");
    options.set(ProtoConfluentFormatOptions.IS_KEY, false);
    options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, onDeserializeError);
    return options;
  }

  private static KafkaProducer<byte[], byte[]> deadLetterProducerOf(
      ProtoRowDataDeserializationSchema schema) throws ReflectiveOperationException {
    Field field = ProtoRowDataDeserializationSchema.class.getDeclaredField("deadLetterProducer");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    KafkaProducer<byte[], byte[]> producer =
        (KafkaProducer<byte[], byte[]>) field.get(schema);
    return producer;
  }

  @Test
  void deadLetterTopicWithoutBootstrapServers_doesNotCreateProducer()
      throws IOException, ReflectiveOperationException {
    Configuration options = baseOptions("skip");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    // Deliberately no 'dead-letter.properties' at all.
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);

    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(listener));

    assertNull(
        deadLetterProducerOf(schema),
        "No dead-letter producer may be created without 'bootstrap.servers'");

    // The record is still dropped and counted: the misconfiguration degrades the DLQ only.
    assertNull(schema.deserialize(POISON));
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void deadLetterPropertiesWithoutBootstrapServers_doesNotCreateProducer()
      throws IOException, ReflectiveOperationException {
    Configuration options = baseOptions("skip");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    // Properties are present but 'bootstrap.servers' is missing — same degraded outcome.
    options.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES, Map.of("security.protocol", "SSL"));
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);

    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(listener));

    assertNull(deadLetterProducerOf(schema));
    assertNull(schema.deserialize(POISON));
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void deadLetterTopicWithBootstrapServers_createsProducer()
      throws IOException, ReflectiveOperationException {
    // Control case, so the two assertions above cannot pass for the wrong reason (e.g. the field
    // being renamed or the producer never being created at all). The producer is constructed
    // lazily-connected, so an unreachable broker address is fine here.
    Configuration options = baseOptions("skip");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    options.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
        Map.of("bootstrap.servers", "localhost:9092"));
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);

    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(new MetricListener()));

    KafkaProducer<byte[], byte[]> producer = deadLetterProducerOf(schema);
    assertNotNull(producer, "A dead-letter producer is expected once 'bootstrap.servers' is set");
    producer.close(Duration.ZERO);
  }

  @Test
  void deadLetterTopicUnset_doesNotCreateProducer()
      throws IOException, ReflectiveOperationException {
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(baseOptions("skip"));
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(new MetricListener()));
    assertNull(deadLetterProducerOf(schema));
  }

  @Test
  void invalidOnDeserializeErrorValue_behavesAsSkip() throws IOException {
    // Documented behavior (issue #71): the option is not validated and only "fail" is fatal, so a
    // typo silently disables fail-fast — the record is counted and dropped instead.
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(baseOptions("bogus"));

    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(listener));

    assertNull(schema.deserialize(POISON));
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void emptyOnDeserializeErrorValue_behavesAsSkip() throws IOException {
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(baseOptions(""));
    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(listener));
    assertNull(schema.deserialize(POISON));
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void failOnDeserializeErrorValue_isCaseInsensitive() {
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(baseOptions("FaIl"));
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(new MetricListener()));
    assertThrows(IOException.class, () -> schema.deserialize(POISON));
  }
}
