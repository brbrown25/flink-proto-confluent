package com.bbrownsound.flink.formats.proto.registry.confluent;

import java.util.Map;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoConfluentFormatOptions {

  /** Schema Registry URL. */
  public static final ConfigOption<String> URL =
      ConfigOptions.key("url")
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("schema-registry.url")
          .withDescription("The URL of the Confluent Schema Registry to fetch/register schemas.");

  /** Kafka topic name for schema subject. */
  public static final ConfigOption<String> TOPIC =
      ConfigOptions.key("topic")
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("topic")
          .withDescription("The kafka topic name");

  /** Whether the format is used for the key. */
  public static final ConfigOption<Boolean> IS_KEY =
      ConfigOptions.key("is_key")
          .booleanType()
          .defaultValue(false)
          .withFallbackKeys("is_key")
          .withDescription("is this format used on a kafka key, default false");

  /** Whether to auto-register schemas. */
  public static final ConfigOption<Boolean> AUTO_REGISTER_SCHEMAS =
      ConfigOptions.key("auto-register-schemas")
          .booleanType()
          .defaultValue(false)
          .withFallbackKeys("auto_register_schemas")
          .withDescription("should the format attempt at registering schema");

  /** Whether to normalize schemas. */
  public static final ConfigOption<Boolean> NORMALIZE_SCHEMAS =
      ConfigOptions.key("normalize-schemas")
          .booleanType()
          .defaultValue(true)
          .withFallbackKeys("normalize_schemas")
          .withDescription("should the format attempt at normalizing schema");

  /** Schema ID to use for serialization (-1 for auto). */
  public static final ConfigOption<Integer> USE_SCHEMA_ID =
      ConfigOptions.key("use-schema-id")
          .intType()
          .defaultValue(-1)
          .withFallbackKeys("use_schema_id")
          .withDescription("schema ID to be used for serialization");

  /** Whether to skip known types. */
  public static final ConfigOption<Boolean> SKIP_KNOWN_TYPES =
      ConfigOptions.key("skip-known-types")
          .booleanType()
          .defaultValue(true)
          .withFallbackKeys("skip_known_types")
          .withDescription("should the format skip known types");

  /** SSL keystore location. */
  public static final ConfigOption<String> SSL_KEYSTORE_LOCATION =
      ConfigOptions.key("ssl.keystore.location")
          .stringType()
          .noDefaultValue()
          .withDescription("Location / File of SSL keystore");

  /** SSL keystore password. */
  public static final ConfigOption<String> SSL_KEYSTORE_PASSWORD =
      ConfigOptions.key("ssl.keystore.password")
          .stringType()
          .noDefaultValue()
          .withDescription("Password for SSL keystore");

  /** SSL truststore location. */
  public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION =
      ConfigOptions.key("ssl.truststore.location")
          .stringType()
          .noDefaultValue()
          .withDescription("Location / File of SSL truststore");

  /** SSL truststore password. */
  public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
      ConfigOptions.key("ssl.truststore.password")
          .stringType()
          .noDefaultValue()
          .withDescription("Password for SSL truststore");

  /** Basic auth credentials source. */
  public static final ConfigOption<String> BASIC_AUTH_CREDENTIALS_SOURCE =
      ConfigOptions.key("basic-auth.credentials-source")
          .stringType()
          .noDefaultValue()
          .withDescription("Basic auth credentials source for Schema Registry");

  /** Basic auth user info for Schema Registry. */
  public static final ConfigOption<String> BASIC_AUTH_USER_INFO =
      ConfigOptions.key("basic-auth.user-info")
          .stringType()
          .noDefaultValue()
          .withDescription("Basic auth user info for schema registry");

  /** Bearer auth credentials source. */
  public static final ConfigOption<String> BEARER_AUTH_CREDENTIALS_SOURCE =
      ConfigOptions.key("bearer-auth.credentials-source")
          .stringType()
          .noDefaultValue()
          .withDescription("Bearer auth credentials source for Schema Registry");

  /** Bearer auth token. */
  public static final ConfigOption<String> BEARER_AUTH_TOKEN =
      ConfigOptions.key("bearer-auth.token")
          .stringType()
          .noDefaultValue()
          .withDescription("Bearer auth token for Schema Registry");

  /** Optional properties forwarded to Schema Registry. */
  public static final ConfigOption<Map<String, String>> PROPERTIES =
      ConfigOptions.key("properties")
          .mapType()
          .noDefaultValue()
          .withDescription(
              "Properties map that is forwarded to the underlying Schema Registry. "
                  + "This is useful for options that are not officially exposed "
                  + "via Flink config options. However, note that Flink options "
                  + "have higher precedence.");

  private ProtoConfluentFormatOptions() {}
}
