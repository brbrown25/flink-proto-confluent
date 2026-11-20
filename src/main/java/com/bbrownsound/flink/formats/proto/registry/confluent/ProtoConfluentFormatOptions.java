package com.bbrownsound.flink.formats.proto.registry.confluent;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.DeadLetterDelivery;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import java.time.Duration;
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
          .withDescription(
              "The Kafka topic the subject name is derived from. When unset, the table's own "
                  + "'topic' option (the Kafka connector's) is used if it names exactly one "
                  + "topic. Not needed when 'subject' is set.");

  /** Explicit Schema Registry subject, bypassing the subject-naming strategy. */
  public static final ConfigOption<String> SUBJECT =
      ConfigOptions.key("subject")
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The Schema Registry subject to register and look up schemas under, e.g. "
                  + "'orders-value'. When set, no subject-naming strategy is applied and "
                  + "'topic' is not required. Cannot be combined with 'subject-name-strategy'.");

  /** Subject-naming strategy for the key or value, depending on 'is_key'. */
  public static final ConfigOption<String> SUBJECT_NAME_STRATEGY =
      ConfigOptions.key("subject-name-strategy")
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("subject_name_strategy")
          .withDescription(
              "How the subject name is derived: 'topic' (<topic>-key / <topic>-value, the "
                  + "Confluent default), 'record' (the fully-qualified message name), "
                  + "'topic-record' (<topic>-<message name>), case-insensitive, or the "
                  + "fully-qualified name of a class implementing Confluent's "
                  + "SubjectNameStrategy. Applies to the key when 'is_key' is true and to the "
                  + "value otherwise, and wins over a strategy set through 'properties'. Cannot "
                  + "be combined with 'subject'.");

  /** Whether the format is used for the key. */
  public static final ConfigOption<Boolean> IS_KEY =
      ConfigOptions.key("is_key")
          .booleanType()
          .defaultValue(false)
          .withFallbackKeys("is_key")
          .withDescription("is this format used on a kafka key, default false");

  /**
   * Fully-qualified generated protobuf message class used to serialize/register instead of a
   * dynamic (Row-derived) schema.
   */
  public static final ConfigOption<String> MESSAGE_CLASS =
      ConfigOptions.key("message-class")
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("message_class")
          .withDescription(
              "Fully-qualified generated protobuf message class (e.g. "
                  + "'com.example.MyProto$MyMessage') to use for serialization and schema "
                  + "registration. When set, the format serializes with this named message's "
                  + "descriptor instead of deriving a dynamic schema from the Flink Row type. "
                  + "Applies to the key when 'is_key' is true and to the value otherwise, so a "
                  + "sink can pin an explicit named entity for its key and/or value. When unset, "
                  + "the format falls back to the previous dynamic-schema behavior. On a source, "
                  + "the table's columns are checked against this message when the query is "
                  + "planned (or when the source opens, if the class is only on the task's "
                  + "classpath), and records whose writer message is a different message are "
                  + "handled per 'on-deserialize-error'.");

  /** Fully-qualified protobuf message name every record read by a source must carry. */
  public static final ConfigOption<String> MESSAGE_NAME =
      ConfigOptions.key("message-name")
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("message_name")
          .withDescription(
              "Source only. Fully-qualified protobuf message name (e.g. 'com.example.Order') "
                  + "that every record's writer message must have. A record of any other message "
                  + "is a deserialize error and is handled per 'on-deserialize-error'. Needs no "
                  + "class on the classpath. When 'message-class' is also set, both must name "
                  + "the same message. Ignored by a sink.");

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

  /** How long a sink reuses the latest schema it looked up for its subject. */
  public static final ConfigOption<Duration> SCHEMA_CACHE_TTL =
      ConfigOptions.key("schema-cache.ttl")
          .durationType()
          .noDefaultValue()
          .withFallbackKeys("schema_cache.ttl")
          .withDescription(
              "Sink only. How long the latest schema looked up for the subject is reused before "
                  + "the Schema Registry is asked again, e.g. '5 min'. Must be positive. When "
                  + "unset (the default) the sink keeps the schema it resolved at its first write "
                  + "until the job restarts. If a refresh fails, the previously resolved schema "
                  + "keeps being used and the refresh is retried.");

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

  /**
   * What to do when a record fails to deserialize: {@code skip} (drop + count) or {@code fail}
   * (throw). A Schema Registry outage always fails the task (GitHub issue #232). An enum option, so
   * any other value is rejected when a query using the table is planned; values are matched
   * case-insensitively.
   */
  public static final ConfigOption<OnDeserializeError> ON_DESERIALIZE_ERROR =
      ConfigOptions.key("on-deserialize-error")
          .enumType(OnDeserializeError.class)
          .defaultValue(OnDeserializeError.FAIL)
          .withFallbackKeys("on_deserialize_error")
          .withDescription(
              "Behavior when a record cannot be deserialized: 'fail' (throw and fail the task — "
                  + "the default, preserving existing semantics) or 'skip' (log, increment the "
                  + "numDeserializeErrors metric, optionally route to the dead-letter topic, and "
                  + "return null so a single poison record cannot crash-loop the job). Set 'skip' "
                  + "(ideally with 'dead-letter-topic') for resilient pipelines. A Schema "
                  + "Registry that cannot be reached or fails (I/O error, 5xx, 401, 403, 429) "
                  + "fails the task with either value and the record is not dead-lettered; a "
                  + "schema ID the registry says does not exist (404) is a poison record. Values "
                  + "are case-insensitive; any other value is rejected with a ValidationException.");

  /** Optional dead-letter topic for records that fail to deserialize. */
  public static final ConfigOption<String> DEAD_LETTER_TOPIC =
      ConfigOptions.key("dead-letter-topic")
          .stringType()
          .noDefaultValue()
          .withFallbackKeys("dead_letter_topic")
          .withDescription(
              "Optional Kafka topic to which the raw bytes of records that fail to deserialize "
                  + "are produced (with error metadata in headers). Requires "
                  + "'dead-letter.properties' to contain at least 'bootstrap.servers'; when it "
                  + "does not, the table's own 'properties.bootstrap.servers' is used, and if "
                  + "that is absent too the table is rejected with a ValidationException. When "
                  + "unset, failed records are only logged and counted.");

  /**
   * Producer properties for the dead-letter topic (bootstrap.servers + security). Forwarded
   * verbatim to the KafkaProducer; key/value serializers are set to ByteArraySerializer in code.
   */
  public static final ConfigOption<Map<String, String>> DEAD_LETTER_PROPERTIES =
      ConfigOptions.key("dead-letter.properties")
          .mapType()
          .noDefaultValue()
          .withDescription(
              "Properties forwarded verbatim to the dead-letter KafkaProducer (e.g. "
                  + "bootstrap.servers, security.protocol, sasl.mechanism, sasl.jaas.config). "
                  + "Only used when 'dead-letter-topic' is set.");

  /**
   * Whether a dead-letter record is acknowledged by the broker before the source moves on ({@code
   * sync}) or merely handed to the producer ({@code async}). An enum option, so any other value is
   * rejected when a query using the table is planned; values are matched case-insensitively.
   */
  public static final ConfigOption<DeadLetterDelivery> DEAD_LETTER_DELIVERY =
      ConfigOptions.key("dead-letter.delivery")
          .enumType(DeadLetterDelivery.class)
          .defaultValue(DeadLetterDelivery.ASYNC)
          .withFallbackKeys("dead_letter.delivery")
          .withDescription(
              "How a record is delivered to the dead-letter topic: 'async' (the default: hand "
                  + "it to the producer and continue; a failed send is logged and counted in "
                  + "numDeadLetterErrors, but the record may be lost) or 'sync' (wait for the "
                  + "broker to acknowledge it before the source moves on; a failed send fails "
                  + "the task instead of dropping the record). Values are case-insensitive; any "
                  + "other value is rejected with a ValidationException.");

  private ProtoConfluentFormatOptions() {}
}
