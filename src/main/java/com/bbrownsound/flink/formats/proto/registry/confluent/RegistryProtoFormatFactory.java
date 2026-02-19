package com.bbrownsound.flink.formats.proto.registry.confluent;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
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

  @Override
  public DecodingFormat<org.apache.flink.api.common.serialization.DeserializationSchema<RowData>>
      createDecodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    String url = formatOptions.get(ProtoConfluentFormatOptions.URL);
    String topic = formatOptions.get(ProtoConfluentFormatOptions.TOPIC);
    LOG.debug("[proto-confluent] Creating decoding format: url={}, topic={}", url, topic);
    return new ProtoDecodingFormat(new ProtoConfluentFormatConfig(formatOptions));
  }

  @Override
  public EncodingFormat<org.apache.flink.api.common.serialization.SerializationSchema<RowData>>
      createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    return new ProtoEncodingFormat(new ProtoConfluentFormatConfig(formatOptions));
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
            ProtoConfluentFormatOptions.IS_KEY,
            ProtoConfluentFormatOptions.PROPERTIES,
            ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION,
            ProtoConfluentFormatOptions.SSL_KEYSTORE_PASSWORD,
            ProtoConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION,
            ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD,
            ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE,
            ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO,
            ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE,
            ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN)
        .collect(Collectors.toSet());
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(ProtoConfluentFormatOptions.URL);
    options.add(ProtoConfluentFormatOptions.TOPIC);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(ProtoConfluentFormatOptions.IS_KEY);
    options.add(ProtoConfluentFormatOptions.PROPERTIES);
    options.add(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS);
    options.add(ProtoConfluentFormatOptions.NORMALIZE_SCHEMAS);
    options.add(ProtoConfluentFormatOptions.USE_SCHEMA_ID);
    options.add(ProtoConfluentFormatOptions.SKIP_KNOWN_TYPES);
    options.add(ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION);
    options.add(ProtoConfluentFormatOptions.SSL_KEYSTORE_PASSWORD);
    options.add(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION);
    options.add(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD);
    options.add(ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE);
    options.add(ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO);
    options.add(ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE);
    options.add(ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN);
    return options;
  }

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

    formatOptions
        .getOptional(ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION)
        .ifPresent(v -> properties.put("schema.registry.ssl.keystore.location", v));
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.SSL_KEYSTORE_PASSWORD)
        .ifPresent(v -> properties.put("schema.registry.ssl.keystore.password", v));
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION)
        .ifPresent(v -> properties.put("schema.registry.ssl.truststore.location", v));
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD)
        .ifPresent(v -> properties.put("schema.registry.ssl.truststore.password", v));
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE)
        .ifPresent(v -> properties.put("basic.auth.credentials.source", v));
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO)
        .ifPresent(v -> properties.put("basic.auth.user.info", v));
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE)
        .ifPresent(v -> properties.put("bearer.auth.credentials.source", v));
    formatOptions
        .getOptional(ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN)
        .ifPresent(v -> properties.put("bearer.auth.token", v));

    if (properties.isEmpty()) {
      return null;
    }
    return properties;
  }
}
