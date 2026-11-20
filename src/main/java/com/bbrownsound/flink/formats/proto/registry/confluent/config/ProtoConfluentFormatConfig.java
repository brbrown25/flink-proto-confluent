package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.RegistryProtoFormatFactory;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import com.bbrownsound.flink.formats.proto.registry.confluent.subject.FixedSubjectNameStrategy;
import com.bbrownsound.flink.formats.proto.registry.confluent.subject.SubjectNameStrategies;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoConfluentFormatConfig implements Serializable {
  /**
   * Pinned to the default UID this class had in 1.2.0, so a config serialized by 1.2.0 still
   * deserializes. Fields may be added or removed (Java serialization ignores removed ones and
   * leaves added ones at their default), but do not change this value.
   */
  private static final long serialVersionUID = -1324640499131780698L;

  private static final Logger LOG = LoggerFactory.getLogger(ProtoConfluentFormatConfig.class);

  /** Schema Registry base URL. */
  public String schemaRegistryUrl;

  /**
   * Kafka topic name for schema subject resolution: the format's {@code topic} option, or else the
   * table's single topic; {@code null} when neither is available (only valid with {@link
   * #subject}).
   */
  public String topic;

  /** Explicit Schema Registry subject from the {@code subject} option, or {@code null}. */
  public String subject;

  /** Whether this format is used for the key (vs value). */
  public Boolean isKey;

  /** Behavior on deserialize failure: {@link OnDeserializeError#FAIL} (default) or SKIP. */
  public OnDeserializeError onDeserializeError = OnDeserializeError.FAIL;

  /** Optional dead-letter topic for records that fail to deserialize (null = disabled). */
  public String deadLetterTopic;

  /**
   * How dead-letter records are delivered: {@link DeadLetterDelivery#ASYNC} (default) or SYNC.
   */
  public DeadLetterDelivery deadLetterDelivery = DeadLetterDelivery.ASYNC;

  /** Producer properties for the dead-letter topic (bootstrap.servers + security). */
  private Map<String, String> deadLetterProperties = new HashMap<>();

  /** Optional properties passed to Schema Registry client. */
  private Map<String, String> properties;

  /**
   * How long a sink reuses the latest schema it looked up for its subject before asking the Schema
   * Registry again; {@code null} (the default) keeps it until the job restarts.
   */
  public Duration schemaCacheTtl;

  /**
   * Fully-qualified protobuf message name every record read by a source must carry, and the message
   * of the subject's schema a sink writes; {@code null} (the default) accepts any message on a
   * source and writes the schema's first message on a sink.
   */
  public String messageName;

  /**
   * Fully-qualified name of the message a sink derives from the table's row type; {@code null}
   * (as in a config from an earlier release) means {@code com.flink.proto.confluent.Row}.
   */
  public String derivedMessageName;

  /**
   * Constructs config from explicit parameters.
   *
   * @param schemaRegistryURL Schema Registry base URL
   * @param topic Kafka topic name for schema subject
   * @param isKey whether this format is for the key
   * @param properties optional properties for the Schema Registry client
   */
  public ProtoConfluentFormatConfig(
      String schemaRegistryURL, String topic, Boolean isKey, Map<String, String> properties) {
    this.schemaRegistryUrl = schemaRegistryURL;
    this.topic = topic;
    this.isKey = isKey;
    this.properties = properties == null ? new HashMap<>() : new HashMap<>(properties);
  }

  /**
   * Constructs config from Flink format options.
   *
   * @param formatOptions the format options from the table environment
   * @throws org.apache.flink.table.api.ValidationException if {@code on-deserialize-error} is set
   *     to anything other than {@code fail} or {@code skip}, or {@code dead-letter.delivery} to
   *     anything other than {@code async} or {@code sync} (both case-insensitive)
   */
  public ProtoConfluentFormatConfig(ReadableConfig formatOptions) {
    this(formatOptions, null);
  }

  /**
   * Constructs config from Flink format options, with a broker list for the dead-letter topic to
   * fall back to.
   *
   * @param formatOptions the format options from the table environment
   * @param defaultDeadLetterBootstrapServers {@code bootstrap.servers} to use for the dead-letter
   *     producer when a dead-letter topic is set and {@code dead-letter.properties} does not name
   *     any; {@code null} for no fallback
   * @throws org.apache.flink.table.api.ValidationException as for {@link
   *     #ProtoConfluentFormatConfig(ReadableConfig)}
   */
  public ProtoConfluentFormatConfig(
      ReadableConfig formatOptions, String defaultDeadLetterBootstrapServers) {
    this(formatOptions, defaultDeadLetterBootstrapServers, null);
  }

  /**
   * Constructs config from Flink format options, with fallbacks taken from the table's own
   * (connector) options.
   *
   * @param formatOptions the format options from the table environment
   * @param defaultDeadLetterBootstrapServers as for {@link
   *     #ProtoConfluentFormatConfig(ReadableConfig, String)}
   * @param defaultTopic the topic to derive the subject from when the format's {@code topic}
   *     option is unset, typically the Kafka connector's single {@code topic}; {@code null} for no
   *     fallback
   * @throws org.apache.flink.table.api.ValidationException as for {@link
   *     #ProtoConfluentFormatConfig(ReadableConfig)}
   */
  public ProtoConfluentFormatConfig(
      ReadableConfig formatOptions, String defaultDeadLetterBootstrapServers, String defaultTopic) {
    Boolean autoRegister = formatOptions.get(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS);
    Boolean normalize = formatOptions.get(ProtoConfluentFormatOptions.NORMALIZE_SCHEMAS);
    Integer schemaId = formatOptions.get(ProtoConfluentFormatOptions.USE_SCHEMA_ID);
    Boolean skipKnown = formatOptions.get(ProtoConfluentFormatOptions.SKIP_KNOWN_TYPES);
    schemaRegistryUrl = formatOptions.get(ProtoConfluentFormatOptions.URL);
    topic = formatOptions.getOptional(ProtoConfluentFormatOptions.TOPIC).orElse(defaultTopic);
    subject = formatOptions.getOptional(ProtoConfluentFormatOptions.SUBJECT).orElse(null);
    isKey = formatOptions.get(ProtoConfluentFormatOptions.IS_KEY);
    onDeserializeError = OnDeserializeError.fromOptions(formatOptions);
    deadLetterTopic =
        formatOptions.getOptional(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC).orElse(null);
    deadLetterDelivery = DeadLetterDelivery.fromOptions(formatOptions);
    deadLetterProperties = new HashMap<>();
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES)
        .ifPresent(deadLetterProperties::putAll);
    if (deadLetterTopic != null && defaultDeadLetterBootstrapServers != null) {
      deadLetterProperties.putIfAbsent("bootstrap.servers", defaultDeadLetterBootstrapServers);
    }

    schemaCacheTtl =
        formatOptions.getOptional(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL).orElse(null);
    messageName =
        formatOptions
            .getOptional(ProtoConfluentFormatOptions.MESSAGE_NAME)
            .filter(v -> !v.isEmpty())
            .orElse(null);
    derivedMessageName = formatOptions.get(ProtoConfluentFormatOptions.DERIVED_MESSAGE_NAME);

    properties = RegistryProtoFormatFactory.buildOptionalPropertiesMap(formatOptions);
    if (properties == null) {
      properties = new HashMap<>();
    }
  properties.put("schema.registry.url", schemaRegistryUrl);
  properties.put("auto.register.schemas", autoRegister.toString());
  properties.put("normalize.schemas", normalize.toString());
  properties.put("use.schema.id", schemaId.toString());
  properties.put("skip.known.types", skipKnown.toString());
    routeSubjectNaming(formatOptions);
    // An explicit message-class is role-scoped: route it to the key or value serializer property
    // based on is_key so the serializer picks it up. A value already tunneled in via the raw
    // 'properties' map (e.g. 'value.message-class:...') is preserved when this option is unset.
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.MESSAGE_CLASS)
        .filter(v -> !v.isEmpty())
        .ifPresent(
            v ->
                properties.put(
                    Boolean.TRUE.equals(isKey) ? "key.message-class" : "value.message-class", v));
    LOG.debug(
        "[proto-confluent] FormatConfig from options: schemaRegistryURL={}, topic={}, "
            + "subject={}, isKey={}, propertiesKeys={}",
        SchemaRegistryClients.withoutUserInfo(schemaRegistryUrl),
        topic,
        subject,
        isKey,
        properties.keySet());
  }

  /**
   * Routes the typed subject options to the key or value serde property, overriding a strategy
   * tunneled in through the raw {@code properties} map. An explicit {@code subject} installs
   * {@link FixedSubjectNameStrategy}; otherwise {@code subject-name-strategy}, when set, selects
   * the strategy class.
   */
  private void routeSubjectNaming(ReadableConfig formatOptions) {
    boolean key = Boolean.TRUE.equals(isKey);
    String strategyKey = SubjectNameStrategies.strategyConfigKey(key);
    if (subject != null) {
      properties.put(strategyKey, FixedSubjectNameStrategy.class.getName());
      properties.put(
          key
              ? FixedSubjectNameStrategy.KEY_SUBJECT_CONFIG
              : FixedSubjectNameStrategy.VALUE_SUBJECT_CONFIG,
          subject);
      return;
    }
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY)
        .ifPresent(v -> properties.put(strategyKey, SubjectNameStrategies.className(v)));
  }

  /**
   * Returns a copy of the serializer/deserializer properties map.
   *
   * @return copy of the properties map, never null
   */
  public Map<String, String> getProperties() {
    return properties == null ? Collections.emptyMap() : new HashMap<>(properties);
  }

  /**
   * Returns the message class pinned for this format's role: {@code message-class}, or a {@code
   * key.message-class} / {@code value.message-class} tunneled through {@code properties}.
   *
   * @return the class name, or {@code null} when none is set
   */
  public String getMessageClass() {
    // Every constructor sets a non-null properties map.
    final String value =
        properties.get(Boolean.TRUE.equals(isKey) ? "key.message-class" : "value.message-class");
    return value == null || value.isEmpty() ? null : value;
  }

  /**
   * Returns a copy of the dead-letter producer properties map.
   *
   * @return copy of the dead-letter producer properties, never null
   */
  public Map<String, String> getDeadLetterProperties() {
    return deadLetterProperties == null
        ? Collections.emptyMap()
        : new HashMap<>(deadLetterProperties);
  }
}
