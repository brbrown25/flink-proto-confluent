package com.bbrownsound.flink.formats.proto.registry.confluent;

import com.bbrownsound.flink.formats.proto.registry.confluent.subject.SubjectNameStrategies;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

/**
 * Checks that the key and value formats of one Kafka table do not resolve the same Schema
 * Registry subject (GitHub issue #241). A format only sees its own options, so the check reads the
 * table's options under each format's prefix, the way Flink's {@code TableFactoryHelper} builds
 * them: {@code key.proto-confluent.} for the connector's {@code key.format}, and {@code
 * value.proto-confluent.} for {@code value.format} or {@code proto-confluent.} for {@code format}.
 */
final class KeyValueSubjects {

  private static final String KEY_FORMAT = "key.format";
  private static final String VALUE_FORMAT = "value.format";
  private static final String FORMAT = "format";
  private static final String KEY_PREFIX = "key." + RegistryProtoFormatFactory.IDENTIFIER + ".";

  private KeyValueSubjects() {}

  /**
   * Rejects a table whose {@code key.format} is this format without {@code is_key = true}, and a
   * table whose key and value formats both resolve the same subject when it can be known at
   * planning time (an explicit {@code subject}, or the topic strategy with a known topic).
   *
   * @param tableOptions all options of the table
   * @param tableTopic the table's single Kafka topic, or {@code null}
   * @throws ValidationException if the key format would resolve a value subject, or if both
   *     formats resolve one subject
   */
  static void validate(Map<String, String> tableOptions, @Nullable String tableTopic) {
    if (!RegistryProtoFormatFactory.IDENTIFIER.equals(tableOptions.get(KEY_FORMAT))) {
      return;
    }
    Configuration table = Configuration.fromMap(tableOptions);
    ReadableConfig keyOptions = new DelegatingConfiguration(table, KEY_PREFIX);
    String keyIsKey = tableOptions.get(KEY_PREFIX + ProtoConfluentFormatOptions.IS_KEY.key());
    if (!isTrue(keyIsKey)) {
      String asValue =
          keyOptions.getOptional(ProtoConfluentFormatOptions.SUBJECT).isPresent()
              ? null
              : subject(keyOptions, false, tableTopic);
      throw new ValidationException(
          "The table's '"
              + KEY_FORMAT
              + "' is '"
              + RegistryProtoFormatFactory.IDENTIFIER
              + "' but '"
              + KEY_PREFIX
              + ProtoConfluentFormatOptions.IS_KEY.key()
              + "' is "
              + (keyIsKey == null ? "not set" : "set to '" + keyIsKey + "'")
              + ", so the key format "
              + (asValue == null
                  ? "would resolve its subject as a value subject"
                  : "would resolve the value subject '" + asValue + "'")
              + ": a sink would register the key schema there and a source would check keys "
              + "against it. Add '"
              + KEY_PREFIX
              + ProtoConfluentFormatOptions.IS_KEY.key()
              + "' = 'true'.");
    }
    String valuePrefix = valuePrefix(tableOptions);
    if (valuePrefix == null) {
      return;
    }
    String valueIsKeyOption = valuePrefix + ProtoConfluentFormatOptions.IS_KEY.key();
    String keySubject = subject(keyOptions, true, tableTopic);
    String valueSubject =
        subject(
            new DelegatingConfiguration(table, valuePrefix),
            isTrue(tableOptions.get(valueIsKeyOption)),
            tableTopic);
    if (keySubject != null && keySubject.equals(valueSubject)) {
      throw new ValidationException(
          "The table's key format and value format both resolve the Schema Registry subject '"
              + keySubject
              + "', so a sink would register the key schema and the value schema as versions of "
              + "one subject and a source would check keys and values against it. Leave '"
              + valueIsKeyOption
              + "' unset or 'false', and give the key and value formats different '"
              + ProtoConfluentFormatOptions.SUBJECT.key()
              + "' values.");
    }
  }

  /** The prefix of the value format's options, or {@code null} if it is not this format. */
  private static @Nullable String valuePrefix(Map<String, String> tableOptions) {
    if (RegistryProtoFormatFactory.IDENTIFIER.equals(tableOptions.get(VALUE_FORMAT))) {
      return "value." + RegistryProtoFormatFactory.IDENTIFIER + ".";
    }
    if (RegistryProtoFormatFactory.IDENTIFIER.equals(tableOptions.get(FORMAT))) {
      return RegistryProtoFormatFactory.IDENTIFIER + ".";
    }
    return null;
  }

  /**
   * The subject a format resolves, when it is known before any record is written: an explicit
   * {@code subject}, or {@code <topic>-key} / {@code <topic>-value} under the topic strategy (the
   * default) with a known topic. Any other strategy depends on the record, so returns {@code
   * null}.
   */
  private static @Nullable String subject(
      ReadableConfig formatOptions, boolean isKey, @Nullable String tableTopic) {
    Optional<String> subject = formatOptions.getOptional(ProtoConfluentFormatOptions.SUBJECT);
    if (subject.isPresent()) {
      return subject.get();
    }
    String strategy =
        formatOptions
            .getOptional(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY)
            .map(SubjectNameStrategies::className)
            .orElseGet(
                () ->
                    formatOptions
                        .getOptional(ProtoConfluentFormatOptions.PROPERTIES)
                        .map(p -> p.get(SubjectNameStrategies.strategyConfigKey(isKey)))
                        .orElse(null));
    if (strategy != null && !TopicNameStrategy.class.getName().equals(strategy)) {
      return null;
    }
    String topic = formatOptions.getOptional(ProtoConfluentFormatOptions.TOPIC).orElse(tableTopic);
    return topic == null ? null : topic + (isKey ? "-key" : "-value");
  }

  private static boolean isTrue(@Nullable String value) {
    return value != null && Boolean.parseBoolean(value.trim());
  }
}
