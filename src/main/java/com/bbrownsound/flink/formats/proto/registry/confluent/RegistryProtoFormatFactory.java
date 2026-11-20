package com.bbrownsound.flink.formats.proto.registry.confluent;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.DeadLetterDelivery;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.SourceMessageValidator;
import com.bbrownsound.flink.formats.proto.registry.confluent.subject.SubjectNameStrategies;
import java.util.Arrays;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class RegistryProtoFormatFactory
    implements DeserializationFormatFactory, SerializationFormatFactory {
  private static final Logger LOG = LoggerFactory.getLogger(RegistryProtoFormatFactory.class);

  /** Format identifier for table options (e.g. 'value.format' = 'proto-confluent'). */
  public static final String IDENTIFIER = "proto-confluent";

  /** Producer property naming the brokers of the dead-letter topic. */
  private static final String DEAD_LETTER_BOOTSTRAP_SERVERS = "bootstrap.servers";

  /** The Kafka connector's table option naming the brokers of the table's own topic. */
  private static final String TABLE_BOOTSTRAP_SERVERS = "properties.bootstrap.servers";

  /**
   * The Kafka connector's table option naming the table's topic, or several separated by {@code
   * ;}.
   */
  private static final String TABLE_TOPIC = "topic";

  /** Creates the factory; invoked by the Flink service loader. */
  public RegistryProtoFormatFactory() {}

  @Override
  public DecodingFormat<org.apache.flink.api.common.serialization.DeserializationSchema<RowData>>
      createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    // Checked first so that a bad value is reported with the allowed values, rather than as the
    // generic "Invalid value for option" that validateFactoryOptions raises for it.
    OnDeserializeError.fromOptions(formatOptions);
    DeadLetterDelivery.fromOptions(formatOptions);
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    // Sink-only, but a declared option of this factory: reject a bad value on a source table too,
    // rather than only once the same table is written to.
    validateSchemaCacheTtl(formatOptions);
    validateMessageName(formatOptions);
    ProtoConfluentFormatConfig config = createConfig(context, formatOptions);
    LOG.debug(
        "[proto-confluent] Creating decoding format: url={}, topic={}, subject={}",
        SchemaRegistryClients.withoutUserInfo(config.schemaRegistryUrl),
        config.topic,
        config.subject);
    return new ProtoDecodingFormat(
        config,
        context == null
            ? Thread.currentThread().getContextClassLoader()
            : context.getClassLoader());
  }

  @Override
  public EncodingFormat<org.apache.flink.api.common.serialization.SerializationSchema<RowData>>
      createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    // The option only affects the source, but it is a declared option of this factory, so a bad
    // value is rejected on a sink too; report it with the allowed values there as well.
    OnDeserializeError.fromOptions(formatOptions);
    DeadLetterDelivery.fromOptions(formatOptions);
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    validateSchemaCacheTtl(formatOptions);
    validateMessageName(formatOptions);

    return new ProtoEncodingFormat(createConfig(context, formatOptions));
  }

  /**
   * Builds the format config and makes sure a configured dead-letter topic has a broker to write
   * to. A missing {@code bootstrap.servers} in {@code dead-letter.properties} is filled in from
   * the table's own {@code properties.bootstrap.servers} (the Kafka connector's option) when the
   * table has one, and is rejected otherwise: it used to degrade to a WARN at open, which dropped
   * poison records without dead-lettering them.
   */
  private static ProtoConfluentFormatConfig createConfig(
      @Nullable DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    String tableBootstrapServers =
        context == null
            ? null
            : context.getCatalogTable().getOptions().get(TABLE_BOOTSTRAP_SERVERS);
    String tableTopicOption =
        context == null ? null : context.getCatalogTable().getOptions().get(TABLE_TOPIC);
    validateSubjectNaming(context, formatOptions);
    if (context != null) {
      KeyValueSubjects.validate(
          context.getCatalogTable().getOptions(), singleTopic(tableTopicOption));
    }
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            formatOptions, tableBootstrapServers, singleTopic(tableTopicOption));
    requireTopicOrSubject(config, tableTopicOption);
    if (config.deadLetterTopic != null
        && !config.getDeadLetterProperties().containsKey(DEAD_LETTER_BOOTSTRAP_SERVERS)) {
      throw new ValidationException(
          "'"
              + ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC.key()
              + "' is set to '"
              + config.deadLetterTopic
              + "' but '"
              + ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES.key()
              + "' has no '"
              + DEAD_LETTER_BOOTSTRAP_SERVERS
              + "' and the table has no '"
              + TABLE_BOOTSTRAP_SERVERS
              + "' to fall back to. Set the brokers of the dead-letter topic, or remove '"
              + ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC.key()
              + "'.");
    }
    return config;
  }

  /**
   * Returns the topic named by the Kafka connector's {@code topic} table option when it names
   * exactly one, else {@code null}: a list of topics has no single subject to derive.
   */
  static @Nullable String singleTopic(@Nullable String tableTopicOption) {
    if (tableTopicOption == null) {
      return null;
    }
    List<String> topics =
        Arrays.stream(tableTopicOption.split(";"))
            .map(String::trim)
            .filter(t -> !t.isEmpty())
            .collect(Collectors.toList());
    return topics.size() == 1 ? topics.get(0) : null;
  }

  /**
   * Rejects a blank {@code subject}, {@code subject} together with {@code subject-name-strategy}
   * (an explicit subject leaves no strategy to apply, so one of them would be silently ignored),
   * and a strategy that names no loadable {@code SubjectNameStrategy}.
   */
  private static void validateSubjectNaming(
      @Nullable DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    Optional<String> subject = formatOptions.getOptional(ProtoConfluentFormatOptions.SUBJECT);
    Optional<String> strategy =
        formatOptions.getOptional(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY);
    if (subject.isPresent() && subject.get().trim().isEmpty()) {
      throw new ValidationException(
          "'"
              + ProtoConfluentFormatOptions.SUBJECT.key()
              + "' must not be empty. Name a Schema Registry subject, or remove the option to "
              + "derive the subject from the topic.");
    }
    if (subject.isPresent() && strategy.isPresent()) {
      throw new ValidationException(
          "'"
              + ProtoConfluentFormatOptions.SUBJECT.key()
              + "' and '"
              + ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY.key()
              + "' cannot both be set: an explicit subject is used as is, so no strategy "
              + "applies. Remove one of them.");
    }
    if (strategy.isPresent()) {
      ClassLoader classLoader =
          context == null
              ? RegistryProtoFormatFactory.class.getClassLoader()
              : context.getClassLoader();
      SubjectNameStrategies.validatedClassName(
          ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY.key(), strategy.get(), classLoader);
    }
  }

  /** The subject is derived from a topic or named outright; a table needs one of the two. */
  private static void requireTopicOrSubject(
      ProtoConfluentFormatConfig config, @Nullable String tableTopicOption) {
    if (config.topic != null || config.subject != null) {
      return;
    }
    String tableTopic =
        tableTopicOption == null
            ? "the table has no single 'topic' to fall back to"
            : "the table's 'topic' ('"
                + tableTopicOption
                + "') names more than one topic, so it cannot be used as a fallback";
    throw new ValidationException(
        "The proto-confluent format needs a topic or a subject to resolve the Schema Registry "
            + "subject, but neither '"
            + ProtoConfluentFormatOptions.TOPIC.key()
            + "' nor '"
            + ProtoConfluentFormatOptions.SUBJECT.key()
            + "' is set and "
            + tableTopic
            + ". Set '"
            + ProtoConfluentFormatOptions.TOPIC.key()
            + "' (e.g. 'value.proto-confluent.topic') or '"
            + ProtoConfluentFormatOptions.SUBJECT.key()
            + "'.");
  }

  private static void validateSchemaCacheTtl(ReadableConfig formatOptions) {
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL)
        .filter(ttl -> ttl.isZero() || ttl.isNegative())
        .ifPresent(
            ttl -> {
              throw new ValidationException(
                  "'"
                      + ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL.key()
                      + "' must be a positive duration, got "
                      + ttl
                      + ". Leave it unset to keep the schema resolved at the first write.");
            });
  }

  /**
   * Rejects a malformed {@code message-name} when the table is planned. Checked on a sink too,
   * where the option is ignored, so a typo surfaces whichever way the table is used first.
   */
  private static void validateMessageName(ReadableConfig formatOptions) {
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.MESSAGE_NAME)
        .ifPresent(
            name ->
                SourceMessageValidator.validateMessageName(
                    ProtoConfluentFormatOptions.MESSAGE_NAME.key(), name));
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> forwardOptions() {
    return Stream.of(
            ProtoConfluentFormatOptions.URL,
            ProtoConfluentFormatOptions.TOPIC,
            ProtoConfluentFormatOptions.SUBJECT,
            ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY,
            ProtoConfluentFormatOptions.IS_KEY,
            ProtoConfluentFormatOptions.MESSAGE_CLASS,
            ProtoConfluentFormatOptions.MESSAGE_NAME,
            ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL,
            ProtoConfluentFormatOptions.PROPERTIES,
            ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION,
            ProtoConfluentFormatOptions.SSL_KEYSTORE_PASSWORD,
            ProtoConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION,
            ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD,
            ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE,
            ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO,
            ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE,
            ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN,
            ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR,
            ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC,
            ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
            ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY)
        .collect(Collectors.toSet());
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(ProtoConfluentFormatOptions.URL);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    // Optional individually, but a table needs 'topic', 'subject' or a single Kafka 'topic' to
    // fall back to; createConfig rejects it otherwise.
    options.add(ProtoConfluentFormatOptions.TOPIC);
    options.add(ProtoConfluentFormatOptions.SUBJECT);
    options.add(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY);
    options.add(ProtoConfluentFormatOptions.IS_KEY);
    options.add(ProtoConfluentFormatOptions.MESSAGE_CLASS);
    options.add(ProtoConfluentFormatOptions.MESSAGE_NAME);
    options.add(ProtoConfluentFormatOptions.PROPERTIES);
    options.add(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS);
    options.add(ProtoConfluentFormatOptions.NORMALIZE_SCHEMAS);
    options.add(ProtoConfluentFormatOptions.USE_SCHEMA_ID);
    options.add(ProtoConfluentFormatOptions.SKIP_KNOWN_TYPES);
    options.add(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL);
    options.add(ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION);
    options.add(ProtoConfluentFormatOptions.SSL_KEYSTORE_PASSWORD);
    options.add(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION);
    options.add(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD);
    options.add(ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE);
    options.add(ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO);
    options.add(ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE);
    options.add(ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN);
    options.add(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR);
    options.add(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC);
    options.add(ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES);
    options.add(ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY);
    return options;
  }

  /**
   * The format options that configure the connection to the registry (SSL and authentication).
   * Their client properties come from {@link SchemaRegistryClients#CONNECTION_OPTIONS}.
   */
  static final List<ConfigOption<String>> CONNECTION_OPTIONS =
      List.of(
          ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION,
          ProtoConfluentFormatOptions.SSL_KEYSTORE_PASSWORD,
          ProtoConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION,
          ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD,
          ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE,
          ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO,
          ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE,
          ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN);

  /**
   * Builds a properties map from optional format options (SSL, auth, etc.) for the Schema Registry
   * client.
   *
   * @param formatOptions the Flink format options
   * @return the properties map, or null if no optional properties are set
   */
  public static @Nullable Map<String, String> buildOptionalPropertiesMap(
      ReadableConfig formatOptions) {
    final Map<String, String> properties = new HashMap<>();

    formatOptions.getOptional(ProtoConfluentFormatOptions.PROPERTIES).ifPresent(properties::putAll);

    // The option-to-client-property mapping is shared with the ddl command line tool.
    for (ConfigOption<String> option : CONNECTION_OPTIONS) {
      formatOptions
          .getOptional(option)
          .ifPresent(v -> properties.put(SchemaRegistryClients.clientProperty(option.key()), v));
    }

    if (properties.isEmpty()) {
      return null;
    }
    return properties;
  }
}
