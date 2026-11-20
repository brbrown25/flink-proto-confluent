package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.DeadLetterDelivery;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.UserCodeClassLoader;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers GitHub issue #171: dead-letter delivery is no longer fire-and-forget. Asserts the send
 * callback (logged and counted in {@code numDeadLetterRecords} / {@code numDeadLetterErrors}), the
 * {@code dead-letter.delivery} = {@code async | sync} semantics, and that the producer is closed
 * through the class loader release hook, {@code close()}, and a failed {@code sync} send.
 *
 * <p>The producer is a Kafka {@link MockProducer}, so each test decides exactly when and how a send
 * completes. The real {@code KafkaProducer} path is covered by {@link
 * ProtoRowDataDeserializationSchemaDeadLetterTest} and the dead-letter integration test.
 */
@DisplayName("ProtoRowDataDeserializationSchema dead-letter delivery (issue #171)")
class ProtoRowDataDeserializationSchemaDeadLetterDeliveryTest {

  private static final byte[] POISON = new byte[] {0, 1, 2, 3, 4};
  private static final String RECORDS = "numDeadLetterRecords";
  private static final String ERRORS = "numDeadLetterErrors";

  // -------------------------------------------------------------------------------------------
  // Metrics registration
  // -------------------------------------------------------------------------------------------

  @Test
  void open_registersBothCountersAtZero_evenWithoutADeadLetterTopic() {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, true);
    f.config.deadLetterTopic = null;
    f.open();
    assertEquals(0L, f.count(RECORDS));
    assertEquals(0L, f.count(ERRORS));
    assertEquals(0L, f.count("numDeserializeErrors"));
    assertNull(f.schema.seenProperties, "No producer is created without a dead-letter topic");
    // The hook is still registered: it closes the Schema Registry client (GitHub issue #248).
    assertEquals(1, f.loader.hooks.size());
    f.loader.hooks.values().forEach(Runnable::run);
    assertFalse(f.producer.closed());
  }

  @Test
  void open_passesDeadLetterPropertiesAndByteArraySerializersToTheProducer() {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, true);
    f.open();
    Properties seen = f.schema.seenProperties;
    assertEquals("localhost:9092", seen.get("bootstrap.servers"));
    assertEquals(ByteArraySerializer.class.getName(), seen.get("key.serializer"));
    assertEquals(ByteArraySerializer.class.getName(), seen.get("value.serializer"));
  }

  // -------------------------------------------------------------------------------------------
  // async (default)
  // -------------------------------------------------------------------------------------------

  @Test
  void async_isTheDefault_andDoesNotWaitForTheAcknowledgement() throws IOException {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, false);
    assertEquals(DeadLetterDelivery.ASYNC, f.config.deadLetterDelivery);
    f.open();

    // The send is still pending (the mock does not auto-complete) and deserialize returns anyway.
    assertNull(f.schema.deserialize(POISON));
    assertEquals(1, f.producer.history().size());
    assertEquals(0L, f.count(RECORDS));
    assertEquals(0L, f.count(ERRORS));

    assertTrue(f.producer.completeNext());
    assertEquals(1L, f.count(RECORDS));
    assertEquals(0L, f.count(ERRORS));

    ProducerRecord<byte[], byte[]> sent = f.producer.history().get(0);
    assertEquals("dlq", sent.topic());
    assertArrayEquals(POISON, sent.value());
    assertEquals("source-topic", header(sent, "source.topic"));
    assertFalse(header(sent, "error.class").isEmpty());
  }

  @Test
  void subjectWithoutTopic_leavesTheSourceTopicHeaderOut() throws IOException {
    // GitHub issue #212: a 'topic-pattern' table names its subject and has no single topic. The
    // format is never told which topic a record came from, so there is no topic to report, and the
    // header used to hold the string "null".
    Fixture f =
        new Fixture(
            null,
            OnDeserializeError.SKIP,
            true,
            new RecordingClassLoader(),
            null,
            "orders-value");
    assertNull(f.config.topic);
    assertEquals("orders-value", f.config.subject);
    f.open();

    assertNull(f.schema.deserialize(POISON));

    ProducerRecord<byte[], byte[]> sent = f.producer.history().get(0);
    assertArrayEquals(POISON, sent.value());
    assertNull(sent.headers().lastHeader("source.topic"), "No topic, so no source.topic header");
    for (Header h : sent.headers()) {
      assertNotEquals("null", new String(h.value(), StandardCharsets.UTF_8), h.key());
    }
    assertFalse(header(sent, "error.class").isEmpty());
    assertFalse(header(sent, "error.message").isEmpty());
    assertEquals(1L, f.count(RECORDS));
  }

  @Test
  void topicAndSubject_keepTheConfiguredTopicInTheSourceTopicHeader() throws IOException {
    Fixture f =
        new Fixture(
            null,
            OnDeserializeError.SKIP,
            true,
            new RecordingClassLoader(),
            "orders",
            "orders-value");
    f.open();

    assertNull(f.schema.deserialize(POISON));

    assertEquals("orders", header(f.producer.history().get(0), "source.topic"));
  }

  @Test
  void async_failedSend_isCountedAndTheRecordIsStillSkipped() throws IOException {
    Fixture f = new Fixture("async", OnDeserializeError.SKIP, false);
    f.open();

    assertNull(f.schema.deserialize(POISON));
    assertTrue(f.producer.errorNext(new TimeoutException("broker gone")));

    assertEquals(0L, f.count(RECORDS));
    assertEquals(1L, f.count(ERRORS));
    assertEquals(1L, f.count("numDeserializeErrors"));
    assertFalse(f.producer.closed(), "An async failure does not tear the producer down");
  }

  @Test
  void async_sendThatThrows_isCountedAndTheRecordIsStillSkipped() throws IOException {
    Fixture f = new Fixture("async", OnDeserializeError.SKIP, false);
    f.open();
    f.producer.sendException = new KafkaException("cannot send");

    assertNull(f.schema.deserialize(POISON));
    assertNull(f.schema.deserialize(POISON));

    assertEquals(0L, f.count(RECORDS));
    assertEquals(2L, f.count(ERRORS));
    assertFalse(f.producer.closed());
  }

  @Test
  void async_withFail_stillThrowsTheDeserializeErrorAfterDeadLettering() {
    Fixture f = new Fixture("async", OnDeserializeError.FAIL, true);
    f.open();

    IOException e = assertThrows(IOException.class, () -> f.schema.deserialize(POISON));
    assertTrue(e.getMessage().startsWith("Proto-confluent deserialize failed"), e.getMessage());
    assertEquals(1L, f.count(RECORDS));
  }

  // -------------------------------------------------------------------------------------------
  // sync
  // -------------------------------------------------------------------------------------------

  @Test
  void sync_acknowledgedSend_skipsTheRecordAndCountsIt() throws IOException {
    Fixture f = new Fixture("sync", OnDeserializeError.SKIP, true);
    assertEquals(DeadLetterDelivery.SYNC, f.config.deadLetterDelivery);
    f.open();

    assertNull(f.schema.deserialize(POISON));

    assertEquals(1L, f.count(RECORDS));
    assertEquals(0L, f.count(ERRORS));
    assertFalse(f.producer.closed());
  }

  @Test
  void sync_blocksUntilTheBrokerAnswers_andFailsTheTaskWhenTheSendFails() throws Exception {
    Fixture f = new Fixture("SYNC", OnDeserializeError.SKIP, false);
    f.open();

    TimeoutException brokerError = new TimeoutException("broker gone");
    Thread broker =
        new Thread(
            () -> {
              // Fail the send only once deserialize() has issued it and is blocked on the result.
              while (!f.producer.errorNext(brokerError)) {
                Thread.onSpinWait();
              }
            });
    broker.start();

    IOException e = assertThrows(IOException.class, () -> f.schema.deserialize(POISON));
    broker.join();

    assertTrue(e.getMessage().contains("'dlq'"), e.getMessage());
    assertTrue(e.getMessage().contains("'dead-letter.delivery' = 'sync'"), e.getMessage());
    assertTrue(e.getMessage().contains("broker gone"), e.getMessage());
    assertSame(brokerError, e.getCause());
    assertEquals(1, e.getSuppressed().length, "The deserialize error is kept as suppressed");
    assertEquals(0L, f.count(RECORDS));
    assertEquals(1L, f.count(ERRORS), "Counted once, by the callback");
    assertEquals(1L, f.count("numDeserializeErrors"));
    assertTrue(f.producer.closed(), "The failing task releases its producer");
  }

  @Test
  void sync_sendThatThrows_failsTheTask_andKeepsFailingAfterwards() {
    Fixture f = new Fixture("sync", OnDeserializeError.SKIP, true);
    f.open();
    KafkaException sendError = new KafkaException("cannot send");
    f.producer.sendException = sendError;

    IOException e = assertThrows(IOException.class, () -> f.schema.deserialize(POISON));
    assertSame(sendError, e.getCause());
    assertEquals(1L, f.count(ERRORS));
    assertTrue(f.producer.closed());

    // A later poison record must not slip through just because the producer is now closed.
    assertThrows(IOException.class, () -> f.schema.deserialize(POISON));
    assertEquals(2L, f.count(ERRORS));
    assertEquals(0L, f.count(RECORDS));
  }

  @Test
  void sync_withFail_reportsTheDeadLetterFailureRatherThanTheDeserializeError() {
    Fixture f = new Fixture("sync", OnDeserializeError.FAIL, true);
    f.open();
    f.producer.sendException = new KafkaException("cannot send");

    IOException e = assertThrows(IOException.class, () -> f.schema.deserialize(POISON));
    assertTrue(e.getMessage().contains("dead-letter delivery"), e.getMessage());
  }

  @Test
  void sync_interruptedWhileWaiting_failsTheTaskAndRestoresTheInterruptFlag() {
    Fixture f = new Fixture("sync", OnDeserializeError.SKIP, false);
    f.open();

    Thread.currentThread().interrupt();
    try {
      IOException e = assertThrows(IOException.class, () -> f.schema.deserialize(POISON));
      assertInstanceOf(InterruptedException.class, e.getCause());
      assertTrue(Thread.currentThread().isInterrupted(), "The interrupt flag must be restored");
    } finally {
      // Clear the flag so it cannot leak into another test on this thread.
      assertTrue(Thread.interrupted());
    }
    assertTrue(f.producer.closed());
    // The outcome of the in-flight record is unknown, so it is not counted here.
    assertEquals(0L, f.count(ERRORS));
    assertEquals(0L, f.count(RECORDS));
  }

  // -------------------------------------------------------------------------------------------
  // Closing the producer
  // -------------------------------------------------------------------------------------------

  @Test
  void open_registersAReleaseHookThatClosesTheProducer() {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, true);
    f.open();

    assertEquals(1, f.loader.hooks.size());
    String name = f.loader.hooks.keySet().iterator().next();
    assertTrue(name.startsWith("proto-confluent-source-"), name);
    assertFalse(f.producer.closed());

    // What Flink does when the job's user-code class loader is released on the TaskManager.
    f.loader.hooks.values().forEach(Runnable::run);
    assertTrue(f.producer.closed());
  }

  @Test
  void eachSchemaInstanceRegistersItsOwnReleaseHook() {
    // Hooks are registered "if absent" by name: a shared name would leave the producers of all
    // but the first parallel subtask on a TaskManager unclosed.
    RecordingClassLoader shared = new RecordingClassLoader();
    Fixture first = new Fixture(null, OnDeserializeError.SKIP, true, shared);
    Fixture second = new Fixture(null, OnDeserializeError.SKIP, true, shared);
    first.open();
    second.open();

    assertEquals(2, shared.hooks.size());
    shared.hooks.values().forEach(Runnable::run);
    assertTrue(first.producer.closed());
    assertTrue(second.producer.closed());
  }

  @Test
  void open_isIdempotent_andDoesNotCreateASecondProducerOrHook() {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, true);
    f.open();
    f.open();
    assertEquals(1, f.schema.producersCreated);
    assertEquals(1, f.loader.hooks.size());
  }

  @Test
  void close_closesTheProducer_andCanBeCalledAgain() {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, true);
    f.open();
    f.schema.close();
    assertTrue(f.producer.closed());
    assertDoesNotThrow(f.schema::close);
  }

  @Test
  void close_worksAsTryWithResources_withoutAnInitializationContext() throws IOException {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, true);
    try (ProtoRowDataDeserializationSchema schema = f.schema) {
      // No context: no metric group and no class loader to hook into; the counters still work.
      schema.open(null);
      assertNull(schema.deserialize(POISON));
      assertEquals(1, f.producer.history().size());
    }
    assertTrue(f.producer.closed());
    assertTrue(f.loader.hooks.isEmpty());
  }

  @Test
  void close_beforeOpen_andWithoutADeadLetterTopic_isANoOp() {
    Fixture unopened = new Fixture(null, OnDeserializeError.SKIP, true);
    assertDoesNotThrow(unopened.schema::close);

    Fixture noTopic = new Fixture(null, OnDeserializeError.SKIP, true);
    noTopic.config.deadLetterTopic = null;
    noTopic.open();
    assertDoesNotThrow(noTopic.schema::close);
    assertFalse(noTopic.producer.closed());
  }

  @Test
  void close_swallowsAProducerCloseFailure() {
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, true);
    f.open();
    f.producer.closeException = new KafkaException("close failed");
    // A release hook or a failing task must not be derailed by the producer refusing to close.
    assertDoesNotThrow(f.schema::close);
  }

  @Test
  void deadLetterDelivery_nullOnTheConfig_behavesAsAsync() throws IOException {
    // The field is public; anything that is not SYNC must not block or fail the task.
    Fixture f = new Fixture(null, OnDeserializeError.SKIP, false);
    f.config.deadLetterDelivery = null;
    f.open();
    assertNull(f.schema.deserialize(POISON));
    assertNotEquals(0, f.producer.history().size());
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static String header(ProducerRecord<byte[], byte[]> record, String key) {
    return new String(record.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
  }

  /** A schema wired to a {@link MockProducer}, a metric listener and a hook-recording loader. */
  private static final class Fixture {
    final ProtoConfluentFormatConfig config;
    final MockProducer<byte[], byte[]> producer;
    final MockedProducerSchema schema;
    final MetricListener listener = new MetricListener();
    final RecordingClassLoader loader;

    Fixture(String delivery, OnDeserializeError onError, boolean autoComplete) {
      this(delivery, onError, autoComplete, new RecordingClassLoader());
    }

    Fixture(
        String delivery,
        OnDeserializeError onError,
        boolean autoComplete,
        RecordingClassLoader loader) {
      this(delivery, onError, autoComplete, loader, "source-topic", null);
    }

    Fixture(
        String delivery,
        OnDeserializeError onError,
        boolean autoComplete,
        RecordingClassLoader loader,
        String topic,
        String subject) {
      Configuration options = new Configuration();
      options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
      if (topic != null) {
        options.set(ProtoConfluentFormatOptions.TOPIC, topic);
      }
      if (subject != null) {
        options.set(ProtoConfluentFormatOptions.SUBJECT, subject);
      }
      options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, onError);
      options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
      options.set(
          ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
          Map.of("bootstrap.servers", "localhost:9092"));
      if (delivery != null) {
        // Raw text, the way a table option arrives from DDL.
        options.setString(ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY.key(), delivery);
      }
      this.config = new ProtoConfluentFormatConfig(options);
      this.producer =
          new MockProducer<>(
              autoComplete, null, new ByteArraySerializer(), new ByteArraySerializer());
      this.schema =
          new MockedProducerSchema(
              (RowType)
                  ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor()),
              config,
              producer);
      this.loader = loader;
    }

    void open() {
      schema.open(
          new DeserializationSchema.InitializationContext() {
            @Override
            public MetricGroup getMetricGroup() {
              return listener.getMetricGroup();
            }

            @Override
            public UserCodeClassLoader getUserCodeClassLoader() {
              return loader;
            }
          });
    }

    long count(String counter) {
      return listener.getCounter(counter).orElseThrow().getCount();
    }
  }

  /** Substitutes the mock for the real {@code KafkaProducer}. */
  private static final class MockedProducerSchema extends ProtoRowDataDeserializationSchema {
    private static final long serialVersionUID = 1L;

    private final transient MockProducer<byte[], byte[]> producer;
    private transient Properties seenProperties;
    private transient int producersCreated;

    MockedProducerSchema(
        RowType rowType, ProtoConfluentFormatConfig config, MockProducer<byte[], byte[]> producer) {
      super(rowType, null, config);
      this.producer = producer;
    }

    @Override
    Producer<byte[], byte[]> createDeadLetterProducer(Properties producerProps) {
      seenProperties = producerProps;
      producersCreated++;
      return producer;
    }
  }

  /** Records release hooks the way Flink's user-code class loader does (first name wins). */
  private static final class RecordingClassLoader implements UserCodeClassLoader {
    final Map<String, Runnable> hooks = new LinkedHashMap<>();

    @Override
    public ClassLoader asClassLoader() {
      return ProtoRowDataDeserializationSchemaDeadLetterDeliveryTest.class.getClassLoader();
    }

    @Override
    public void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
      hooks.putIfAbsent(releaseHookName, releaseHook);
    }
  }
}
