package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
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

  /** Proto package of the Row-derived message; empty when the configured name has no package. */
  private final String derivedPackageName;

  /** Simple name of the Row-derived message. */
  private final String derivedMessageName;

  /** The Row-derived descriptor for the last Row type seen, so it is built once, not per record. */
  private volatile RowDerivedSchema rowDerivedSchema;

  /**
   * When set via {@value #KEY_MESSAGE_CLASS_CONFIG} (key) or {@value #VALUE_MESSAGE_CLASS_CONFIG}
   * (value), used for serialization and registration in place of a dynamic schema.
   */
  private volatile WriteSchema messageClassSchema;

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
    this(client, schemaCacheTtl, nanoClock, null);
  }

  /**
   * Creates a serializer whose cached subject schema expires and whose Row-derived message has the
   * given name.
   *
   * @param client the Schema Registry client
   * @param schemaCacheTtl how long a looked-up subject schema is reused before the registry is
   *     asked again; {@code null} keeps it until the job restarts
   * @param nanoClock monotonic clock in nanoseconds, e.g. {@code System::nanoTime}
   * @param derivedMessageName fully-qualified name of the message derived from the table, e.g.
   *     {@code com.example.Order}; {@code null} for {@code com.flink.proto.confluent.Row}
   */
  RowDataProtoSerializer(
      SchemaRegistryClient client,
      @Nullable Duration schemaCacheTtl,
      LongSupplier nanoClock,
      @Nullable String derivedMessageName) {
    super(client);
    this.converters = new ConcurrentHashMap<>();
    this.schemaCache = new ConcurrentHashMap<>();
    this.schemaCacheTtlNanos = toTtlNanos(schemaCacheTtl);
    this.nanoClock = nanoClock;
    String fullName =
        derivedMessageName == null
            ? ProtoConfluentFormatOptions.DERIVED_MESSAGE_NAME.defaultValue()
            : derivedMessageName;
    int lastDot = fullName.lastIndexOf('.');
    this.derivedPackageName = lastDot < 0 ? "" : fullName.substring(0, lastDot);
    this.derivedMessageName = fullName.substring(lastDot + 1);
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

  /**
   * Configures the serializer, loading a message class with the thread's context class loader (or
   * this class's loader when the thread has none).
   */
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    configure(configs, isKey, null);
  }

  /**
   * Configures the serializer, loading a message class with the given class loader.
   *
   * @param configs serializer configuration
   * @param isKey whether this serializer encodes the record key
   * @param classLoader loader for {@value #KEY_MESSAGE_CLASS_CONFIG} / {@value
   *     #VALUE_MESSAGE_CLASS_CONFIG}, normally the job's user-code class loader so a class shipped
   *     in the job JAR is found; {@code null} uses the thread's context class loader
   */
  void configure(Map<String, ?> configs, boolean isKey, @Nullable ClassLoader classLoader) {
    super.configure(configs, isKey);
    // A single format instance serves either the key or the value, so select the matching config
    // key. This keeps key and value message classes independent for a table that pins both.
    String configKey = isKey ? KEY_MESSAGE_CLASS_CONFIG : VALUE_MESSAGE_CLASS_CONFIG;
    Object messageClassObj = configs.get(configKey);
    if (messageClassObj instanceof String) {
      String className = (String) messageClassObj;
      if (!className.isEmpty()) {
        try {
          Descriptors.Descriptor descriptor =
              descriptorFromMessageClass(
                  className,
                  classLoader != null
                      ? classLoader
                      : Optional.ofNullable(Thread.currentThread().getContextClassLoader())
                          .orElse(RowDataProtoSerializer.class.getClassLoader()));
          this.messageClassSchema = WriteSchema.unregistered(descriptor);
          LOG.info(
              "[proto-confluent] Using message class for {} serialization: {} -> {}",
              isKey ? "key" : "value",
              className,
              descriptor.getFullName());
        } catch (Exception e) {
          throw new ValidationException(
              "Failed to resolve descriptor for " + configKey + "=" + className, e);
        }
      }
    }
  }

  private static Descriptors.Descriptor descriptorFromMessageClass(
      String className, ClassLoader classLoader) throws ReflectiveOperationException {
    Class<?> clazz = Class.forName(className, true, classLoader);
    Method getDefault = clazz.getMethod("getDefaultInstance");
    Message defaultInstance = (Message) getDefault.invoke(null);
    return defaultInstance.getDescriptorForType();
  }

  /**
   * Resolves the schema to serialize with. An explicit message class always wins. Otherwise the
   * schema is the latest one registered under the subject that the configured subject-naming
   * strategy derives from the topic. Strategies that name the subject after the record rather than
   * the topic (e.g. {@code RecordNameStrategy}) cannot produce a subject before a schema exists, so
   * in that case the Row-derived schema is used and the underlying serializer resolves the subject
   * from the message it is handed.
   */
  private WriteSchema resolveSchema(String topic, RowType rowType) {
    WriteSchema messageClass = messageClassSchema;
    if (messageClass != null) {
      return messageClass;
    }
    String subject = getSubjectName(topic, isKey, null, null);
    if (subject == null) {
      return rowDerived(rowType);
    }
    CachedSchema cached = schemaCache.get(subject);
    if (cached != null && schemaCacheTtlNanos == 0L) {
      // No TTL: the entry never expires, so the clock is not read on the per-record path.
      return cached.schema;
    }
    long now = nanoClock.getAsLong();
    if (cached != null && !cached.isDue(now)) {
      return cached.schema;
    }
    return lookupSchema(subject, rowType, cached, now);
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
  private WriteSchema lookupSchema(
      String subject, RowType rowType, @Nullable CachedSchema cached, long now) {
    try {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      if (cached != null && cached.isSameSchema(schemaMetadata)) {
        CachedSchema refreshed = cached.confirmedAt(now, schemaCacheTtlNanos);
        schemaCache.put(subject, refreshed);
        return refreshed.schema;
      }
      WriteSchema resolved = toWriteSchema(subject, schemaMetadata);
      schemaCache.put(
          subject,
          new CachedSchema(
              resolved,
              schemaMetadata.getId(),
              schemaMetadata.getVersion(),
              now,
              schemaCacheTtlNanos));
      if (cached != null) {
        converters.remove(cached.schema.descriptor);
        LOG.info(
            "[proto-confluent] Subject {} moved from schema id {} (version {}) to id {} "
                + "(version {}); serializing with the new schema.",
            subject,
            cached.schemaId,
            cached.version,
            schemaMetadata.getId(),
            schemaMetadata.getVersion());
      }
      return resolved;
    } catch (RestClientException e) {
      if (e.getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE && cached == null) {
        return rowDerived(rowType);
      }
      // A subject that disappears after the sink resolved a schema from it (cached != null) is
      // not a reason to switch to the Row-derived schema mid-stream, let alone register it: an
      // operator deleting the subject to re-register it would find the sink's guess in its place.
      // Nor is the ID read before the deletion still known to be valid under the subject: until
      // the subject is back, the parent looks the schema up (or registers it) as for any write.
      CachedSchema current =
          e.getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE ? cached.withoutConfirmedId() : cached;
      return staleOrFail(
          subject, current, e, "HTTP status " + e.getStatus() + ", error code " + e.getErrorCode());
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

  /**
   * Parses the registered schema, with its references, into the schema the sink writes with. The
   * registered schema and its ID are kept along with the descriptor, so the record is written under
   * that ID (see {@link #serializeRegistered}).
   */
  private WriteSchema toWriteSchema(String subject, SchemaMetadata schemaMetadata)
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
    ProtobufSchema registered = (ProtobufSchema) latestVersion;
    Descriptors.Descriptor desc = registered.toDescriptor();
    if (desc == null) {
      throw new ValidationException("Could not find schema for subject: " + subject);
    }
    return WriteSchema.registered(desc, subject, registered, schemaMetadata.getId());
  }

  /**
   * Handles a registry failure that does not mean "subject not found". With a previously resolved
   * descriptor at hand the failure is logged and that descriptor keeps being served, and the next
   * attempt is put off by the TTL or {@link #REFRESH_RETRY_BACKOFF}, whichever is shorter. Without
   * one the failure is rethrown with the registry exception as its cause; nothing is cached, so the
   * next record asks the registry again.
   */
  private WriteSchema staleOrFail(
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
      return cached.schema;
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

  private WriteSchema rowDerived(RowType rowType) {
    RowDerivedSchema current = rowDerivedSchema;
    if (current != null && current.rowType.equals(rowType)) {
      return current.schema;
    }
    Descriptors.Descriptor desc =
        RowTypeToProto.fromRowType(rowType, derivedMessageName, derivedPackageName);
    if (current != null) {
      converters.remove(current.schema.descriptor);
    }
    WriteSchema schema = WriteSchema.unregistered(desc);
    rowDerivedSchema = new RowDerivedSchema(rowType, schema);
    return schema;
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
    WriteSchema schema = resolveSchema(topic, rowType);
    RowDataToProtoConverters.RowDataToProtoConverter converter =
        converters.computeIfAbsent(
            schema.descriptor, d -> RowDataToProtoConverters.createConverter(rowType, d));
    DynamicMessage message = (DynamicMessage) converter.convert(row);
    if (schema.registered == null) {
      return serialize(topic, message);
    }
    return serializeRegistered(topic, schema, message);
  }

  /**
   * Writes a record with a schema the sink read from the registry, under the ID the registry gave
   * for it.
   *
   * <p>The parent serializer would otherwise rebuild a schema from the message's descriptor and ask
   * the registry for its ID, by registering it (auto-registration on) or by looking it up. The
   * rebuilt text need not match the registered text: Confluent renders a {@code map} field of a
   * descriptor as a repeated {@code option map_entry} message, and with {@code normalize-schemas}
   * (on by default) a lookup did not even find the registered text (cp-schema-registry 7.5.0).
   * The lookup then fails with "Schema not found" (error code 40403), and registration adds a new
   * version (#210). The schema already came from the subject, so there is nothing to look up or
   * register: the record is written as if {@code use-schema-id} named that ID.
   *
   * <p>The parent still picks the ID, from the registered schema rather than a rebuilt one, when
   * {@code use-schema-id} or {@code use.latest.with.metadata} is configured, and while the subject
   * the schema was read from is gone from the registry (see {@link #lookupSchema}).
   */
  private synchronized byte[] serializeRegistered(
      String topic, WriteSchema schema, DynamicMessage message) {
    if (schema.schemaId < 0 || useSchemaId >= 0 || metadata != null) {
      return serializeImpl(schema.subject, topic, isKey, null, message, schema.registered);
    }
    boolean autoRegister = autoRegisterSchema;
    autoRegisterSchema = false;
    useSchemaId = schema.schemaId;
    try {
      return serializeImpl(schema.subject, topic, isKey, null, message, schema.registered);
    } finally {
      useSchemaId = -1;
      autoRegisterSchema = autoRegister;
    }
  }

  /** Number of cached converters; exposed for tests that check the cache stays bounded. */
  int converterCacheSize() {
    return converters.size();
  }

  /**
   * A descriptor to encode rows with and, when it was read from the registry, the subject, the
   * schema as registered there and its ID.
   */
  private static final class WriteSchema {
    private final Descriptors.Descriptor descriptor;
    @Nullable
    private final String subject;
    @Nullable
    private final ProtobufSchema registered;
    private final int schemaId;

    private WriteSchema(
        Descriptors.Descriptor descriptor,
        @Nullable String subject,
        @Nullable ProtobufSchema registered,
        int schemaId) {
      this.descriptor = descriptor;
      this.subject = subject;
      this.registered = registered;
      this.schemaId = schemaId;
    }

    /** A schema the sink did not read from the registry: a message class or the Row type. */
    static WriteSchema unregistered(Descriptors.Descriptor descriptor) {
      return new WriteSchema(descriptor, null, null, -1);
    }

    /** The schema registered under {@code subject} with ID {@code schemaId}. */
    static WriteSchema registered(
        Descriptors.Descriptor descriptor, String subject, ProtobufSchema schema, int schemaId) {
      return new WriteSchema(descriptor, subject, schema, schemaId);
    }

    /** This schema with another ID; {@code -1} when no ID is known to be valid for the subject. */
    WriteSchema withSchemaId(int id) {
      return id == schemaId ? this : new WriteSchema(descriptor, subject, registered, id);
    }
  }

  /** The latest schema of a subject as last read from the registry. */
  private static final class CachedSchema {
    private final WriteSchema schema;
    private final int schemaId;
    private final int version;
    private final long checkedAtNanos;

    /** Nanoseconds after {@link #checkedAtNanos} at which to ask again; {@code 0} means never. */
    private final long recheckAfterNanos;

    CachedSchema(
        WriteSchema schema,
        int schemaId,
        int version,
        long checkedAtNanos,
        long recheckAfterNanos) {
      this.schema = schema;
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
      return new CachedSchema(schema, schemaId, version, nowNanos, nextRecheckAfterNanos);
    }

    /** This entry, checked now against a registry that still has the schema under the subject. */
    CachedSchema confirmedAt(long nowNanos, long nextRecheckAfterNanos) {
      return new CachedSchema(
          schema.withSchemaId(schemaId), schemaId, version, nowNanos, nextRecheckAfterNanos);
    }

    /** This entry, serving its schema without an ID once the subject is gone from the registry. */
    CachedSchema withoutConfirmedId() {
      return new CachedSchema(
          schema.withSchemaId(-1), schemaId, version, checkedAtNanos, recheckAfterNanos);
    }
  }

  /** A Row type and the schema derived from it. */
  private static final class RowDerivedSchema {
    private final RowType rowType;
    private final WriteSchema schema;

    RowDerivedSchema(RowType rowType, WriteSchema schema) {
      this.rowType = rowType;
      this.schema = schema;
    }
  }
}
