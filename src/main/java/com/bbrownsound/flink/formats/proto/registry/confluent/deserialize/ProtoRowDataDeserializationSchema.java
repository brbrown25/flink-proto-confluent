package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.DeadLetterDelivery;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.metrics.SchemaRegistryMetrics;
import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoRowDataDeserializationSchema
    implements DeserializationSchema<RowData>, AutoCloseable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoRowDataDeserializationSchema.class);
  private static final int LOG_EVERY_N_MESSAGES = 100;
  /** How long {@link #close()} waits for dead-letter records that are still in flight. */
  private static final Duration DEAD_LETTER_CLOSE_TIMEOUT = Duration.ofSeconds(10);

  /** Gauge: number of writer schemas (schema IDs) this source holds a converter for. */
  static final String NUM_CACHED_SCHEMAS = "numCachedSchemas";

  /** Type information for the produced RowData. */
  private final TypeInformation<RowData> resultTypeInfo;
  /** Flink row type for the deserialized data. */
  private final RowType rowType;
  /** Format and Schema Registry config. */
  private final ProtoConfluentFormatConfig formatConfig;
  private transient CachedSchemaRegistryClient client;
  private transient KafkaProtobufDeserializer<DynamicMessage> deserializer;
  private transient Map<Integer, ProtoToRowDataConverters.ProtoToRowDataConverter> converters;
  private transient AtomicLong deserializeCount;
  /** Counts records that failed to deserialize (exposed as the numDeserializeErrors metric). */
  private transient Counter deserializeErrors;
  /** Optional producer for the dead-letter topic; null when DLQ is not configured. */
  private transient Producer<byte[], byte[]> deadLetterProducer;
  /** Counts acknowledged dead-letter records (exposed as the numDeadLetterRecords metric). */
  private transient Counter deadLetterRecords;
  /** Counts failed dead-letter sends (exposed as the numDeadLetterErrors metric). */
  private transient Counter deadLetterErrors;

  /**
   * Creates a deserialization schema for the given row type and config.
   *
   * @param rowType Flink row type for the deserialized data
   * @param resultTypeInfo type information for the produced RowData
   * @param config format and Schema Registry configuration
   */
  public ProtoRowDataDeserializationSchema(
      RowType rowType, TypeInformation<RowData> resultTypeInfo, ProtoConfluentFormatConfig config) {
    this.rowType = rowType;
    this.resultTypeInfo = resultTypeInfo;
    this.formatConfig = config;
  }

  @Override
  public void open(InitializationContext context) {
    if (this.client == null) {
      LOG.info(
          "[proto-confluent] ProtoRowDataDeserializationSchema.open: schemaRegistryURL={}, "
              + "topic={}, isKey={}",
          formatConfig.schemaRegistryUrl,
          formatConfig.topic,
          formatConfig.isKey);

      this.client =
          SchemaRegistryMetrics.createClient(
              formatConfig.schemaRegistryUrl,
              formatConfig.getProperties(),
              context != null ? context.getMetricGroup() : null);
      this.deserializer = new KafkaProtobufDeserializer<>(client);
      this.deserializer.configure(formatConfig.getProperties(), formatConfig.isKey);
      this.converters = new ConcurrentHashMap<>();
      this.deserializeCount = new AtomicLong(0);
      // The send callback runs on the producer's I/O thread, so these two are thread-safe.
      this.deadLetterRecords = new ThreadSafeSimpleCounter();
      this.deadLetterErrors = new ThreadSafeSimpleCounter();
      if (context != null) {
        this.deserializeErrors = context.getMetricGroup().counter("numDeserializeErrors");
        context.getMetricGroup().counter("numDeadLetterRecords", deadLetterRecords);
        context.getMetricGroup().counter("numDeadLetterErrors", deadLetterErrors);
        final Map<Integer, ?> converterCache = this.converters;
        context.getMetricGroup().gauge(NUM_CACHED_SCHEMAS, (Gauge<Integer>) converterCache::size);
      }

      if (formatConfig.deadLetterTopic != null) {
        final Map<String, String> dlqProps = formatConfig.getDeadLetterProperties();
        if (!dlqProps.containsKey("bootstrap.servers")) {
          // The format factory rejects this (or fills it in from the table) when the table is
          // planned; this guards a config that was built without the factory. Dropping poison
          // records without dead-lettering them is never the fallback.
          throw new IllegalStateException(
              "dead-letter topic '"
                  + formatConfig.deadLetterTopic
                  + "' is configured but 'dead-letter.properties' has no 'bootstrap.servers'");
        }
        final Properties producerProps = new Properties();
        producerProps.putAll(dlqProps);
        producerProps.put("key.serializer", ByteArraySerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        this.deadLetterProducer = createDeadLetterProducer(producerProps);
        if (context != null) {
          // DeserializationSchema has no close() hook, so the producer is closed when Flink
          // releases the job's user-code class loader on this TaskManager. The name is unique per
          // instance because a hook is only registered if its name is absent.
          context
              .getUserCodeClassLoader()
              .registerReleaseHookIfAbsent(
                  "proto-confluent-dead-letter-producer-" + UUID.randomUUID(), this::close);
        }
        LOG.info(
            "[proto-confluent] dead-letter producer enabled: topic={}, delivery={}",
            formatConfig.deadLetterTopic,
            formatConfig.deadLetterDelivery);
      }
      LOG.debug("[proto-confluent] deserializer.open: client and deserializer initialized");
    }
  }

  @Override
  public RowData deserialize(byte[] message) throws IOException {
    if (message == null) {
      return null;
    }
    if (deserializeCount == null) {
      deserializeCount = new AtomicLong(0);
    }
    long n = deserializeCount.incrementAndGet();
    if (n <= 5 || (n % LOG_EVERY_N_MESSAGES == 0)) {
      LOG.debug(
          "[proto-confluent] deserialize called #{}: messageLength={}, topic={}",
          n,
          message.length,
          formatConfig.topic);
    }
    try {
      DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(message));
      inputStream.readByte(); // skip magic byte
      int id = inputStream.readInt();
      if (n <= 3) {
        LOG.debug("[proto-confluent] deserialize #{}: schemaId={}", n, id);
      }
      DynamicMessage dynamicMessage = deserializer.deserialize(formatConfig.topic, message);

      ProtoToRowDataConverters.ProtoToRowDataConverter converter =
          Optional.ofNullable(converters.get(id))
              .orElseGet(
                  () -> {
                    var res =
                        ProtoToRowDataConverters.createConverter(
                            dynamicMessage.getDescriptorForType(), rowType);
                    converters.put(id, res);
                    if (n <= 3) {
                      LOG.info(
                          "[proto-confluent] deserialize #{}: created "
                              + "converter for schemaId={}, descriptor={}",
                          n,
                          id,
                          dynamicMessage.getDescriptorForType().getFullName());
                    }
                    return res;
                  });
      RowData rowData = (RowData) converter.convert(dynamicMessage);
      if (n <= 5) {
        LOG.debug("[proto-confluent] message #{} from topic: {}", n, rowData);
      }
      return rowData;
    } catch (Exception e) {
      LOG.error(
          "[proto-confluent] deserialize failed: messageLength={}, topic={}, deserializeCount={}",
          message.length,
          formatConfig.topic,
          n,
          e);
      if (deserializeErrors != null) {
        deserializeErrors.inc();
      }
      if (deadLetterProducer != null) {
        sendToDeadLetterTopic(message, e);
      }
      // Default (FAIL): rethrow so the task fails. Only an explicit on-deserialize-error=skip
      // drops the poison record and keeps the job running, so that a single unparseable record
      // cannot crash-loop the source. Anything that is not SKIP, including a null assigned to
      // the public field, is treated as FAIL: dropping records is never the fallback.
      if (formatConfig.onDeserializeError != OnDeserializeError.SKIP) {
        throw new IOException("Proto-confluent deserialize failed: " + e.getMessage(), e);
      }
      return null;
    }
  }

  /**
   * Creates the dead-letter producer. Package-private so tests can substitute a mock.
   *
   * @param producerProps the dead-letter properties plus the byte-array serializers
   * @return the producer
   */
  Producer<byte[], byte[]> createDeadLetterProducer(Properties producerProps) {
    return new KafkaProducer<>(producerProps);
  }

  /**
   * Produces the raw bytes of a record that failed to deserialize to the dead-letter topic.
   *
   * <p>With {@code async} delivery a failure is logged and counted and the caller carries on. With
   * {@code sync} delivery this blocks until the broker acknowledges the record (bounded by the
   * producer's {@code max.block.ms} and {@code delivery.timeout.ms}) and throws if it does not, so
   * that the source never moves past a record that is neither emitted nor dead-lettered.
   */
  private void sendToDeadLetterTopic(byte[] message, Exception error) throws IOException {
    final ProducerRecord<byte[], byte[]> dlqRecord =
        new ProducerRecord<>(formatConfig.deadLetterTopic, null, message);
    dlqRecord
        .headers()
        .add(new RecordHeader("error.class", utf8(error.getClass().getName())))
        .add(new RecordHeader("error.message", utf8(String.valueOf(error.getMessage()))))
        .add(new RecordHeader("source.topic", utf8(String.valueOf(formatConfig.topic))));
    final boolean sync = formatConfig.deadLetterDelivery == DeadLetterDelivery.SYNC;
    try {
      final Future<RecordMetadata> ack =
          deadLetterProducer.send(dlqRecord, this::onDeadLetterCompletion);
      if (sync) {
        ack.get();
      }
    } catch (ExecutionException dlqEx) {
      // Already logged and counted by the send callback.
      throw deadLetterFailure(dlqEx.getCause(), error);
    } catch (InterruptedException dlqEx) {
      // The task is being cancelled; the callback still accounts for the record's outcome.
      Thread.currentThread().interrupt();
      throw deadLetterFailure(dlqEx, error);
    } catch (RuntimeException dlqEx) {
      // send() itself threw (closed producer, serialization, ...): the callback was not invoked.
      deadLetterErrors.inc();
      LOG.error(
          "[proto-confluent] failed to produce to dead-letter topic {}",
          formatConfig.deadLetterTopic,
          dlqEx);
      if (sync) {
        throw deadLetterFailure(dlqEx, error);
      }
    }
  }

  /** Send callback: counts the outcome of every dead-letter record and logs failures. */
  private void onDeadLetterCompletion(RecordMetadata metadata, Exception exception) {
    if (exception == null) {
      deadLetterRecords.inc();
      return;
    }
    deadLetterErrors.inc();
    LOG.error(
        "[proto-confluent] failed to produce to dead-letter topic {}",
        formatConfig.deadLetterTopic,
        exception);
  }

  /**
   * Builds the exception that fails the task when a {@code sync} dead-letter send fails. The
   * producer is closed first: the task is going down and nothing else would release it before the
   * job leaves this TaskManager.
   */
  private IOException deadLetterFailure(Throwable dlqError, Exception deserializeError) {
    close();
    final IOException failure =
        new IOException(
            "Proto-confluent dead-letter delivery to topic '"
                + formatConfig.deadLetterTopic
                + "' failed with 'dead-letter.delivery' = 'sync'; failing instead of dropping "
                + "the record: "
                + dlqError.getMessage(),
            dlqError);
    failure.addSuppressed(deserializeError);
    return failure;
  }

  /**
   * Closes the dead-letter producer, waiting a bounded time for records that are still in flight.
   * Safe to call more than once, and a no-op when no dead-letter topic is configured.
   *
   * <p>Flink's {@link DeserializationSchema} has no close hook, so {@link #open} registers this
   * method as a release hook on the job's user-code class loader. Code that owns the schema
   * directly (for example a DataStream job or a test) can call it itself.
   */
  @Override
  public void close() {
    final Producer<byte[], byte[]> producer = deadLetterProducer;
    if (producer == null) {
      return;
    }
    try {
      producer.close(DEAD_LETTER_CLOSE_TIMEOUT);
    } catch (RuntimeException closeEx) {
      LOG.warn(
          "[proto-confluent] failed to close the dead-letter producer for topic {}",
          formatConfig.deadLetterTopic,
          closeEx);
    }
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return resultTypeInfo;
  }
}
