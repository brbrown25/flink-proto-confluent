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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.DeadLetterDelivery;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.metrics.SchemaRegistryMetrics;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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

  /**
   * Gauge: number of converters this source holds, one per writer schema ID and message read under
   * that ID; equal to the number of writer schemas (schema IDs) while all records under each ID
   * carry the same message.
   */
  static final String NUM_CACHED_SCHEMAS = "numCachedSchemas";

  /** Type information for the produced RowData. */
  private final TypeInformation<RowData> resultTypeInfo;
  /**
   * Flink row type for the deserialized data, read by column name; or, when {@link #projections} is
   * set, the table's physical row type the projection paths index into.
   */
  private final RowType rowType;
  /**
   * The nested projection the source reads (one path of column indexes into {@link #rowType} per
   * produced column), or {@code null} when the source produces {@link #rowType} itself.
   */
  private final int[][] projections;
  /** Format and Schema Registry config. */
  private final ProtoConfluentFormatConfig formatConfig;
  private transient SchemaRegistryClient client;
  private transient KafkaProtobufDeserializer<DynamicMessage> deserializer;
  /**
   * Row converters by writer schema ID, then by the full name of the message the record holds. A
   * Confluent protobuf schema can define several messages and each record's message indexes pick
   * one, so records of different messages can share a schema ID (GitHub issue #211). Within one ID
   * the full name identifies the message, because a registered schema never changes.
   */
  private transient Map<Integer, Map<String, ProtoToRowDataConverters.ProtoToRowDataConverter>>
      converters;
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
   * Full name every record's writer message must have (from {@code message-name} or {@code
   * message-class}); null when any message is accepted.
   */
  private transient String expectedMessageName;

  /**
   * Creates a deserialization schema for the given row type and config.
   *
   * @param rowType Flink row type for the deserialized data
   * @param resultTypeInfo type information for the produced RowData
   * @param config format and Schema Registry configuration
   */
  public ProtoRowDataDeserializationSchema(
      RowType rowType, TypeInformation<RowData> resultTypeInfo, ProtoConfluentFormatConfig config) {
    this(rowType, null, resultTypeInfo, config);
  }

  /**
   * Creates a deserialization schema that produces a nested projection of a table: one column per
   * projection path, holding the value at the end of that path (for example {@code payload.a}).
   * Only the projected values are converted; the sibling fields of a projected nested field are
   * not. See {@link ProtoToRowDataConverters#createProjectedConverter}.
   *
   * @param physicalRowType the table's physical row type
   * @param projections the projected paths of column indexes into {@code physicalRowType}, one per
   *     produced column, as Flink's {@code Projection} holds them; {@code null} produces {@code
   *     physicalRowType} itself
   * @param resultTypeInfo type information for the produced RowData
   * @param config format and Schema Registry configuration
   */
  public ProtoRowDataDeserializationSchema(
      RowType physicalRowType,
      int[][] projections,
      TypeInformation<RowData> resultTypeInfo,
      ProtoConfluentFormatConfig config) {
    this.rowType = physicalRowType;
    this.projections = copy(projections);
    this.resultTypeInfo = resultTypeInfo;
    this.formatConfig = config;
  }

  private static int[][] copy(int[][] projections) {
    if (projections == null) {
      return null;
    }
    final int[][] copy = new int[projections.length][];
    for (int i = 0; i < projections.length; i++) {
      copy[i] = projections[i].clone();
    }
    return copy;
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

      this.expectedMessageName = resolveExpectedMessage(context);
      this.client = createSchemaRegistryClient(context);
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
        final Map<Integer, Map<String, ProtoToRowDataConverters.ProtoToRowDataConverter>>
            converterCache = this.converters;
        context
            .getMetricGroup()
            .gauge(NUM_CACHED_SCHEMAS, (Gauge<Integer>) () -> cachedConverters(converterCache));
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
      DynamicMessage dynamicMessage = resolveAndDecode(message, id);
      checkMessageType(dynamicMessage, id);

      RowData rowData =
          (RowData) converterFor(id, dynamicMessage.getDescriptorForType(), n).convert(dynamicMessage);
      if (n <= 5) {
        LOG.debug("[proto-confluent] message #{} from topic: {}", n, rowData);
      }
      return rowData;
    } catch (SchemaRegistryUnavailableException e) {
      // Not the record's fault: fail the task whatever on-deserialize-error says, and neither
      // count nor dead-letter the record, so that it is read again after the restart instead of
      // being dropped while the registry is down (GitHub issue #232). The failed registry
      // request itself is counted in schemaRegistryErrors.
      LOG.error(
          "[proto-confluent] Schema Registry unavailable: messageLength={}, topic={}, "
              + "deserializeCount={}",
          message.length,
          formatConfig.topic,
          n,
          e);
      throw e;
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
   * Resolves the record's writer schema (from the client's cache or the registry) and decodes the
   * record with it. A failure that means the registry could not be asked, rather than that the
   * record is bad, is rethrown as a {@link SchemaRegistryUnavailableException}; see {@link
   * SchemaRegistryUnavailableException#isRegistryUnavailable} for the rules.
   */
  private DynamicMessage resolveAndDecode(byte[] message, int schemaId)
      throws SchemaRegistryUnavailableException {
    try {
      return deserializer.deserialize(formatConfig.topic, message);
    } catch (RuntimeException e) {
      if (SchemaRegistryUnavailableException.isRegistryUnavailable(e)) {
        throw new SchemaRegistryUnavailableException(formatConfig.topic, schemaId, e);
      }
      throw e;
    }
  }

  /**
   * Creates the Schema Registry client the deserializer looks writer schemas up with.
   * Package-private so tests can substitute an in-memory registry.
   *
   * @param context the initialization context, or {@code null}
   * @return the client
   */
  SchemaRegistryClient createSchemaRegistryClient(InitializationContext context) {
    return SchemaRegistryMetrics.createClient(
        formatConfig.schemaRegistryUrl,
        formatConfig.getProperties(),
        context != null ? context.getMetricGroup() : null);
  }

  /**
   * Loads {@code message-class} (if set) with the job's user-code class loader, checks the produced
   * row type against it, and returns the message full name records must carry.
   *
   * @throws org.apache.flink.table.api.ValidationException if the class cannot be loaded, the
   *     table does not match it, or it conflicts with {@code message-name}
   */
  private String resolveExpectedMessage(InitializationContext context) {
    final String messageClass = formatConfig.getMessageClass();
    if (messageClass == null) {
      return formatConfig.messageName;
    }
    final ClassLoader classLoader =
        context != null
            ? context.getUserCodeClassLoader().asClassLoader()
            : Optional.ofNullable(Thread.currentThread().getContextClassLoader())
                .orElse(ProtoRowDataDeserializationSchema.class.getClassLoader());
    final Descriptor descriptor;
    try {
      descriptor = SourceMessageValidator.loadDescriptor(messageClass, classLoader);
    } catch (ClassNotFoundException e) {
      throw new ValidationException(
          "'message-class' = '"
              + messageClass
              + "' is not on the classpath of the job. Put the JAR with the generated protobuf "
              + "classes in Flink's lib/ directory or in the job JAR.",
          e);
    }
    final String expected =
        SourceMessageValidator.expectedMessageName(
            formatConfig.messageName, messageClass, descriptor);
    SourceMessageValidator.validateProjection(descriptor, rowType, projections, messageClass);
    LOG.info(
        "[proto-confluent] source checked against message-class {} ({}); records of any other "
            + "message are deserialize errors",
        messageClass,
        expected);
    return expected;
  }

  /**
   * Returns the cached converter for the record's writer schema ID and message, building it on the
   * first record of that pair.
   */
  private ProtoToRowDataConverters.ProtoToRowDataConverter converterFor(
      int schemaId, Descriptor descriptor, long n) {
    Map<String, ProtoToRowDataConverters.ProtoToRowDataConverter> byMessage =
        converters.get(schemaId);
    if (byMessage == null) {
      byMessage = new ConcurrentHashMap<>();
      converters.put(schemaId, byMessage);
    }
    final String messageName = descriptor.getFullName();
    ProtoToRowDataConverters.ProtoToRowDataConverter converter = byMessage.get(messageName);
    if (converter == null) {
      converter = createConverter(descriptor);
      byMessage.put(messageName, converter);
      LOG.info(
          "[proto-confluent] deserialize #{}: created converter for schemaId={}, message={}",
          n,
          schemaId,
          messageName);
    }
    return converter;
  }

  /** The number of converters in the cache, which is what {@code numCachedSchemas} reports. */
  private static int cachedConverters(
      Map<Integer, Map<String, ProtoToRowDataConverters.ProtoToRowDataConverter>> cache) {
    int count = 0;
    for (Map<String, ?> byMessage : cache.values()) {
      count += byMessage.size();
    }
    return count;
  }

  private ProtoToRowDataConverters.ProtoToRowDataConverter createConverter(Descriptor descriptor) {
    return projections == null
        ? ProtoToRowDataConverters.createConverter(descriptor, rowType)
        : ProtoToRowDataConverters.createProjectedConverter(descriptor, rowType, projections);
  }

  /**
   * Rejects a record whose writer message is not the expected one. The deserializer picks the
   * message from the record's own message indexes, so one writer schema ID can carry several
   * message types; the decoded message's name is therefore compared on every record, which costs
   * one string comparison.
   */
  private void checkMessageType(DynamicMessage dynamicMessage, int schemaId)
      throws UnexpectedMessageTypeException {
    if (expectedMessageName == null) {
      return;
    }
    final String actual = dynamicMessage.getDescriptorForType().getFullName();
    if (!expectedMessageName.equals(actual)) {
      throw new UnexpectedMessageTypeException(
          formatConfig.topic, schemaId, actual, expectedMessageName);
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
        .add(new RecordHeader("error.message", utf8(String.valueOf(error.getMessage()))));
    // A value format is handed only the record's bytes, never the topic it was read from, so the
    // header holds the configured topic. A source with 'subject' and no 'topic' (for example a
    // 'topic-pattern' table) has none: the header is left out rather than set to "null" (GitHub
    // issue #212).
    if (formatConfig.topic != null) {
      dlqRecord.headers().add(new RecordHeader("source.topic", utf8(formatConfig.topic)));
    }
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
