package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers GitHub issue #71: negative-path behavior of the error-handling options. Asserts the
 * (previously silent) contract that a {@code dead-letter-topic} without {@code bootstrap.servers}
 * creates no producer. Since issue #170 an unrecognized {@code on-deserialize-error} value no
 * longer degrades to the {@code skip} path: it is rejected with a {@code ValidationException}
 * before a schema can be built, which is asserted here as well.
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
    // Set as raw text, the way a table option arrives, so the value goes through Flink's parsing.
    options.setString(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR.key(), onDeserializeError);
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
  void invalidOnDeserializeErrorValue_isRejectedInsteadOfBehavingAsSkip() {
    // Issue #170: this used to be accepted and to take the skip path, silently dropping poison
    // records. No config, and therefore no schema, can be built from it any more.
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> new ProtoConfluentFormatConfig(baseOptions("bogus")));
    assertTrue(e.getMessage().contains("'bogus'"), e.getMessage());
    assertTrue(e.getMessage().contains("fail, skip"), e.getMessage());
  }

  @Test
  void emptyOnDeserializeErrorValue_isRejectedInsteadOfBehavingAsSkip() {
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> new ProtoConfluentFormatConfig(baseOptions("")));
    assertTrue(e.getMessage().contains("fail, skip"), e.getMessage());
  }

  @Test
  void failOnDeserializeErrorValue_isCaseInsensitive() {
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(baseOptions("FaIl"));
    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(listener));
    assertThrows(IOException.class, () -> schema.deserialize(POISON));
    // A fatal record is still counted before the task fails.
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void skipOnDeserializeErrorValue_isCaseInsensitive() throws IOException {
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(baseOptions("sKiP"));
    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(listener));
    assertNull(schema.deserialize(POISON));
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void unsetOnDeserializeError_failsOnPoisonRecord() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "source-topic");
    var schema =
        new ProtoRowDataDeserializationSchema(
            rowType(), null, new ProtoConfluentFormatConfig(options));
    schema.open(contextFor(new MetricListener()));
    IOException e = assertThrows(IOException.class, () -> schema.deserialize(POISON));
    assertTrue(e.getMessage().startsWith("Proto-confluent deserialize failed"), e.getMessage());
  }

  @Test
  void nullOnDeserializeErrorField_failsRatherThanSkips() {
    // The field is public and mutable. A null must take the safe (fatal) path, never skip.
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081",
            "source-topic",
            false,
            Map.of("schema.registry.url", "http://localhost:8081"));
    config.onDeserializeError = null;
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(new MetricListener()));
    assertThrows(IOException.class, () -> schema.deserialize(POISON));
  }

  @Test
  void explicitConstructor_defaultsToFail() {
    // The (url, topic, isKey, properties) constructor bypasses Flink option parsing entirely; it
    // carries no raw string, so the only way to reach SKIP through it is the enum constant.
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081",
            "source-topic",
            false,
            Map.of("schema.registry.url", "http://localhost:8081"));
    assertEquals(OnDeserializeError.FAIL, config.onDeserializeError);
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(new MetricListener()));
    assertThrows(IOException.class, () -> schema.deserialize(POISON));
  }

  @Test
  void skipWithDeadLetterTopicButNoBootstrapServers_viaFallbackKey_stillSkips()
      throws IOException, ReflectiveOperationException {
    // The interaction with 'dead-letter-topic' is unchanged by the enum: a topic without
    // 'bootstrap.servers' is still accepted, creates no producer, and the record is skipped.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "source-topic");
    options.setString("on_deserialize_error", "SKIP");
    options.setString("dead_letter_topic", "dlq");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals(OnDeserializeError.SKIP, config.onDeserializeError);
    assertEquals("dlq", config.deadLetterTopic);

    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config);
    schema.open(contextFor(listener));
    assertNull(deadLetterProducerOf(schema));
    assertNull(schema.deserialize(POISON));
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void invalidValueWithDeadLetterTopic_isRejected() {
    // A dead-letter topic does not make an unknown value acceptable.
    Configuration options = baseOptions("skipp");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    options.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
        Map.of("bootstrap.servers", "localhost:9092"));
    ValidationException e =
        assertThrows(ValidationException.class, () -> new ProtoConfluentFormatConfig(options));
    assertTrue(e.getMessage().contains("'skipp'"), e.getMessage());
  }
}
