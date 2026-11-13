package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.RowTypeToProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public final class RowDataProtoSerializer extends KafkaProtobufSerializer<DynamicMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataProtoSerializer.class);

  /**
   * Config key for an explicit Protobuf message class used when this serializer encodes the value
   * (i.e. {@code isKey == false}), e.g. for schema registration.
   */
  public static final String VALUE_MESSAGE_CLASS_CONFIG = "value.message-class";

  /**
   * Config key for an explicit Protobuf message class used when this serializer encodes the key
   * (i.e. {@code isKey == true}). Companion to {@link #VALUE_MESSAGE_CLASS_CONFIG} so a Kafka sink
   * can pin an explicit named entity for its key instead of a dynamic schema.
   */
  public static final String KEY_MESSAGE_CLASS_CONFIG = "key.message-class";

  /** Schema Registry error code for "subject not found". */
  static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  /**
   * Upper bound on how long a failed refresh of an already cached schema is left alone before the
   * registry is asked again, so an unreachable registry costs one failed call per interval instead
   * of one per record.
   */
  static final Duration REFRESH_RETRY_BACKOFF = Duration.ofSeconds(30);

  private static final String ROW_MESSAGE_NAME = "Row";
  private static final String ROW_PACKAGE_NAME = "com.flink.proto.confluent";

  /**
   * Converters keyed by descriptor <em>identity</em> ({@link Descriptors.Descriptor} does not
   * override {@code equals}), so two versions of a message that share a full name never share a
   * converter. The entry of a superseded descriptor is removed when it is replaced, which bounds
   * the map by the number of live descriptors.
   */
  private final Map<Descriptors.Descriptor, RowDataToProtoConverters.RowDataToProtoConverter>
      converters;

  /** Latest registered schema per subject, refreshed once {@link #schemaCacheTtlNanos} elapses. */
  private final Map<String, CachedSchema> schemaCache;

  /** Time a cached subject schema stays fresh, in nanoseconds; {@code 0} means it never expires. */
  private final long schemaCacheTtlNanos;

  /** Monotonic nanosecond clock; injectable so tests can move time without sleeping. */
  private final LongSupplier nanoClock;

  /** The Row-derived descriptor for the last Row type seen, so it is built once, not per record. */
  private volatile RowDerivedSchema rowDerivedSchema;

  /**
   * When set via {@value #KEY_MESSAGE_CLASS_CONFIG} (key) or {@value #VALUE_MESSAGE_CLASS_CONFIG}
   * (value), used for serialization and registration in place of a dynamic schema.
   */
  private volatile Descriptors.Descriptor messageClassDescriptor;

  /**
   * Creates a serializer that keeps the first schema it resolves for a subject until the job
   * restarts.
   *
   * @param client the Schema Registry client
   */
  RowDataProtoSerializer(SchemaRegistryClient client) {
    this(client, null, System::nanoTime);
  }

  /**
   * Creates a serializer whose cached subject schema expires.
   *
   * @param client the Schema Registry client
   * @param schemaCacheTtl how long a looked-up subject schema is reused before the registry is
   *     asked again; {@code null} keeps it until the job restarts
   * @param nanoClock monotonic clock in nanoseconds, e.g. {@code System::nanoTime}
   */
  RowDataProtoSerializer(
      SchemaRegistryClient client, @Nullable Duration schemaCacheTtl, LongSupplier nanoClock) {
    super(client);
    this.converters = new ConcurrentHashMap<>();
    this.schemaCache = new ConcurrentHashMap<>();
    this.schemaCacheTtlNanos = toTtlNanos(schemaCacheTtl);
    this.nanoClock = nanoClock;
  }

  private static long toTtlNanos(@Nullable Duration ttl) {
    if (ttl == null) {
      return 0L;
    }
    if (ttl.isZero() || ttl.isNegative()) {
      throw new ValidationException(
          "The schema cache TTL must be a positive duration, got " + ttl + ".");
    }
    try {
      return ttl.toNanos();
    } catch (ArithmeticException e) {
      // Longer than a long of nanoseconds can express (about 292 years): effectively never.
      return 0L;
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    // A single format instance serves either the key or the value, so select the matching config
    // key. This keeps key and value message classes independent for a table that pins both.
    String configKey = isKey ? KEY_MESSAGE_CLASS_CONFIG : VALUE_MESSAGE_CLASS_CONFIG;
    Object messageClassObj = configs.get(configKey);
    if (messageClassObj instanceof String) {
      String className = (String) messageClassObj;
      if (!className.isEmpty()) {
        try {
          this.messageClassDescriptor = descriptorFromMessageClass(className);
          LOG.info(
              "[proto-confluent] Using message class for {} serialization: {} -> {}",
              isKey ? "key" : "value",
              className,
              messageClassDescriptor.getFullName());
        } catch (Exception e) {
          throw new ValidationException(
              "Failed to resolve descriptor for " + configKey + "=" + className, e);
        }
      }
    }
  }

  private static Descriptors.Descriptor descriptorFromMessageClass(String className)
      throws ReflectiveOperationException {
    Class<?> clazz = Class.forName(className);
    Method getDefault = clazz.getMethod("getDefaultInstance");
    Message defaultInstance = (Message) getDefault.invoke(null);
    return defaultInstance.getDescriptorForType();
  }

  /**
   * Resolves the Protobuf descriptor to serialize with. An explicit message class always wins.
   * Otherwise the descriptor comes from the latest schema registered under the subject that the
   * configured subject-naming strategy derives from the topic. Strategies that name the subject
   * after the record rather than the topic (e.g. {@code RecordNameStrategy}) cannot produce a
   * subject before a schema exists, so in that case the Row-derived schema is used and the
   * underlying serializer resolves the subject from the message it is handed.
   */
  private Descriptors.Descriptor resolveDescriptor(String topic, RowType rowType) {
    if (messageClassDescriptor != null) {
      return messageClassDescriptor;
    }
    String subject = getSubjectName(topic, isKey, null, null);
    if (subject == null) {
      return rowDerivedDescriptor(rowType);
    }
    CachedSchema cached = schemaCache.get(subject);
    if (cached != null && schemaCacheTtlNanos == 0L) {
      // No TTL: the entry never expires, so the clock is not read on the per-record path.
      return cached.descriptor;
    }
    long now = nanoClock.getAsLong();
    if (cached != null && !cached.isDue(now)) {
      return cached.descriptor;
    }
    return lookupDescriptor(subject, rowType, cached, now);
  }

  /**
   * Asks the registry for the latest schema of the subject and caches it.
   *
   * <p>The Row-derived schema is used only when the registry answers that the subject does not
   * exist (error code {@value #SUBJECT_NOT_FOUND_ERROR_CODE}). Every other failure (authentication,
   * authorization, timeouts, 5xx, a missing version, a 404 that does not carry that error code, an
   * unparseable schema) is never treated as "no schema": on a first lookup it fails the record, and
   * on a refresh of an expired entry the previously resolved descriptor keeps being served. On a
   * refresh that also holds for "subject not found": a sink that resolved a registry schema never
   * switches to the Row-derived one.
   *
   * @param cached the expired entry being refreshed, or {@code null} on a first lookup
   */
  private Descriptors.Descriptor lookupDescriptor(
      String subject, RowType rowType, @Nullable CachedSchema cached, long now) {
    try {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      if (cached != null && cached.isSameSchema(schemaMetadata)) {
        schemaCache.put(subject, cached.checkedAt(now, schemaCacheTtlNanos));
        return cached.descriptor;
      }
      Descriptors.Descriptor desc = toDescriptor(subject, schemaMetadata);
      schemaCache.put(
          subject,
          new CachedSchema(
              desc, schemaMetadata.getId(), schemaMetadata.getVersion(), now, schemaCacheTtlNanos));
      if (cached != null) {
        converters.remove(cached.descriptor);
        LOG.info(
            "[proto-confluent] Subject {} moved from schema id {} (version {}) to id {} "
                + "(version {}); serializing with the new schema.",
            subject,
            cached.schemaId,
            cached.version,
            schemaMetadata.getId(),
            schemaMetadata.getVersion());
      }
      return desc;
    } catch (RestClientException e) {
      if (e.getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE && cached == null) {
        return rowDerivedDescriptor(rowType);
      }
      // A subject that disappears after the sink resolved a schema from it (cached != null) is
      // not a reason to switch to the Row-derived schema mid-stream, let alone register it: an
      // operator deleting the subject to re-register it would find the sink's guess in its place.
      return staleOrFail(
          subject, cached, e, "HTTP status " + e.getStatus() + ", error code " + e.getErrorCode());
    } catch (IOException e) {
      return staleOrFail(subject, cached, e, "I/O error");
    } catch (RuntimeException e) {
      if (cached == null) {
        throw e;
      }
      // E.g. a new latest version whose descriptor cannot be built: keep the working one.
      return staleOrFail(subject, cached, e, e.getClass().getSimpleName());
    }
  }

  private Descriptors.Descriptor toDescriptor(String subject, SchemaMetadata schemaMetadata)
      throws IOException {
    Optional<ParsedSchema> optSchema =
        schemaRegistry.parseSchema(
            new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                null, schemaMetadata));
    ParsedSchema latestVersion =
        optSchema.orElseThrow(
            () ->
                new IOException(
                    "Invalid schema "
                        + schemaMetadata.getSchema()
                        + " with refs "
                        + schemaMetadata.getReferences()
                        + " of type "
                        + schemaMetadata.getSchemaType()));
    if (!(latestVersion instanceof ProtobufSchema)) {
      throw new IOException(
          "Subject "
              + subject
              + " holds a schema of type "
              + schemaMetadata.getSchemaType()
              + ", not PROTOBUF");
    }
    Descriptors.Descriptor desc = ((ProtobufSchema) latestVersion).toDescriptor();
    if (desc == null) {
      throw new ValidationException("Could not find schema for subject: " + subject);
    }
    return desc;
  }

  /**
   * Handles a registry failure that does not mean "subject not found". With a previously resolved
   * descriptor at hand the failure is logged and that descriptor keeps being served, and the next
   * attempt is put off by the TTL or {@link #REFRESH_RETRY_BACKOFF}, whichever is shorter. Without
   * one the failure is rethrown with the registry exception as its cause; nothing is cached, so the
   * next record asks the registry again.
   */
  private Descriptors.Descriptor staleOrFail(
      String subject, @Nullable CachedSchema cached, Exception cause, String detail) {
    if (cached != null) {
      long retryAfterNanos = Math.min(schemaCacheTtlNanos, REFRESH_RETRY_BACKOFF.toNanos());
      // Count the backoff from now, not from before the call: a lookup that took longer than the
      // backoff to fail (a connect timeout) would otherwise be due again on the very next record.
      schemaCache.put(subject, cached.checkedAt(nanoClock.getAsLong(), retryAfterNanos));
      LOG.warn(
          "[proto-confluent] Could not refresh the schema of subject {} ({}); keeping schema id "
              + "{} (version {}) and retrying in {} ms.",
          subject,
          detail,
          cached.schemaId,
          cached.version,
          TimeUnit.NANOSECONDS.toMillis(retryAfterNanos),
          cause);
      return cached.descriptor;
    }
    throw new SerializationException(
        "Schema Registry lookup of the latest schema for subject '"
            + subject
            + "' failed ("
            + detail
            + "): "
            + cause.getMessage()
            + ". Only a 'subject not found' answer (error code "
            + SUBJECT_NOT_FOUND_ERROR_CODE
            + ") falls back to a schema derived from the table; no schema was registered.",
        cause);
  }

  private Descriptors.Descriptor rowDerivedDescriptor(RowType rowType) {
    RowDerivedSchema current = rowDerivedSchema;
    if (current != null && current.rowType.equals(rowType)) {
      return current.descriptor;
    }
    Descriptors.Descriptor desc =
        RowTypeToProto.fromRowType(rowType, ROW_MESSAGE_NAME, ROW_PACKAGE_NAME);
    if (current != null) {
      converters.remove(current.descriptor);
    }
    rowDerivedSchema = new RowDerivedSchema(rowType, desc);
    return desc;
  }

  /**
   * Serializes a Flink RowData to Confluent wire format (magic byte + schema id + protobuf bytes).
   *
   * @param topic the topic name for subject resolution
   * @param rowType the Flink row type
   * @param row the row data to serialize
   * @return the serialized bytes
   * @throws SerializationException if the registry cannot be asked for the subject's schema (the
   *     registry's exception is the cause) or the record cannot be encoded
   */
  public byte[] serializeRowData(String topic, RowType rowType, RowData row) {
    Descriptors.Descriptor desc = resolveDescriptor(topic, rowType);
    RowDataToProtoConverters.RowDataToProtoConverter converter =
        converters.computeIfAbsent(desc, d -> RowDataToProtoConverters.createConverter(rowType, d));
    return serialize(topic, (DynamicMessage) converter.convert(row));
  }

  /** Number of cached converters; exposed for tests that check the cache stays bounded. */
  int converterCacheSize() {
    return converters.size();
  }

  /** The latest schema of a subject as last read from the registry. */
  private static final class CachedSchema {
    private final Descriptors.Descriptor descriptor;
    private final int schemaId;
    private final int version;
    private final long checkedAtNanos;

    /** Nanoseconds after {@link #checkedAtNanos} at which to ask again; {@code 0} means never. */
    private final long recheckAfterNanos;

    CachedSchema(
        Descriptors.Descriptor descriptor,
        int schemaId,
        int version,
        long checkedAtNanos,
        long recheckAfterNanos) {
      this.descriptor = descriptor;
      this.schemaId = schemaId;
      this.version = version;
      this.checkedAtNanos = checkedAtNanos;
      this.recheckAfterNanos = recheckAfterNanos;
    }

    boolean isDue(long nowNanos) {
      // Subtraction, not comparison of absolute values: System.nanoTime may wrap.
      return recheckAfterNanos > 0 && nowNanos - checkedAtNanos >= recheckAfterNanos;
    }

    boolean isSameSchema(SchemaMetadata metadata) {
      return schemaId == metadata.getId() && version == metadata.getVersion();
    }

    CachedSchema checkedAt(long nowNanos, long nextRecheckAfterNanos) {
      return new CachedSchema(descriptor, schemaId, version, nowNanos, nextRecheckAfterNanos);
    }
  }

  /** A Row type and the descriptor derived from it. */
  private static final class RowDerivedSchema {
    private final RowType rowType;
    private final Descriptors.Descriptor descriptor;

    RowDerivedSchema(RowType rowType, Descriptors.Descriptor descriptor) {
      this.rowType = rowType;
      this.descriptor = descriptor;
    }
  }
}
