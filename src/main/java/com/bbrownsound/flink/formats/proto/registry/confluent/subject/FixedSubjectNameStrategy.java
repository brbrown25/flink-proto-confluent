package com.bbrownsound.flink.formats.proto.registry.confluent.subject;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;

/**
 * A {@link SubjectNameStrategy} that returns a configured subject regardless of the topic or the
 * schema. It backs the format's {@code subject} option: Confluent's serializers have no setting
 * that names the subject directly, only a strategy that derives it, so the format installs this
 * strategy for the key or value and passes the subject in {@value #KEY_SUBJECT_CONFIG} or {@value
 * #VALUE_SUBJECT_CONFIG}.
 */
public class FixedSubjectNameStrategy implements SubjectNameStrategy {

  /** Serializer/deserializer config key holding the subject used for the record key. */
  public static final String KEY_SUBJECT_CONFIG = "key.subject";

  /** Serializer/deserializer config key holding the subject used for the record value. */
  public static final String VALUE_SUBJECT_CONFIG = "value.subject";

  private String keySubject;
  private String valueSubject;

  /** Creates the strategy; Confluent's serde config instantiates it reflectively. */
  public FixedSubjectNameStrategy() {}

  @Override
  public void configure(Map<String, ?> configs) {
    keySubject = stringOrNull(configs.get(KEY_SUBJECT_CONFIG));
    valueSubject = stringOrNull(configs.get(VALUE_SUBJECT_CONFIG));
  }

  private static String stringOrNull(Object value) {
    if (value == null) {
      return null;
    }
    String subject = value.toString().trim();
    return subject.isEmpty() ? null : subject;
  }

  @Override
  public String subjectName(String topic, boolean isKey, ParsedSchema schema) {
    String subject = isKey ? keySubject : valueSubject;
    if (subject == null) {
      String configKey = isKey ? KEY_SUBJECT_CONFIG : VALUE_SUBJECT_CONFIG;
      throw new ConfigException(
          getClass().getName() + " is the " + (isKey ? "key" : "value")
              + " subject name strategy but '" + configKey + "' is not set");
    }
    return subject;
  }
}
