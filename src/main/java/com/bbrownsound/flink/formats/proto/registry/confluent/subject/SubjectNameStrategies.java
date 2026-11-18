package com.bbrownsound.flink.formats.proto.registry.confluent.subject;

import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.table.api.ValidationException;

/**
 * Maps the values of the {@code subject-name-strategy} option to the Confluent strategy class the
 * serializer and deserializer are configured with.
 */
public final class SubjectNameStrategies {

  /** Serde config key of the strategy that names the key subject. */
  public static final String KEY_STRATEGY_CONFIG = "key.subject.name.strategy";

  /** Serde config key of the strategy that names the value subject. */
  public static final String VALUE_STRATEGY_CONFIG = "value.subject.name.strategy";

  private static final Map<String, String> ALIASES =
      Map.of(
          "topic", TopicNameStrategy.class.getName(),
          "record", RecordNameStrategy.class.getName(),
          "topic-record", TopicRecordNameStrategy.class.getName());

  private SubjectNameStrategies() {}

  /**
   * Returns the serde config key that selects the strategy for the key or for the value.
   *
   * @param isKey whether the format serves the record key
   * @return {@value #KEY_STRATEGY_CONFIG} or {@value #VALUE_STRATEGY_CONFIG}
   */
  public static String strategyConfigKey(boolean isKey) {
    return isKey ? KEY_STRATEGY_CONFIG : VALUE_STRATEGY_CONFIG;
  }

  /**
   * Resolves an option value to a strategy class name: {@code topic}, {@code record} and {@code
   * topic-record} (case-insensitive) name Confluent's built-in strategies, and anything else is
   * taken as a fully-qualified class name.
   *
   * @param value the option value
   * @return the fully-qualified strategy class name
   */
  public static String className(String value) {
    String trimmed = value.trim();
    String alias = ALIASES.get(trimmed.toLowerCase(Locale.ROOT));
    return alias != null ? alias : trimmed;
  }

  /**
   * Resolves an option value like {@link #className(String)} and checks that the class can be
   * loaded and implements {@link SubjectNameStrategy}, so a typo is reported when the table is
   * planned rather than when the job starts.
   *
   * @param optionKey the option key, for the error message
   * @param value the option value
   * @param classLoader the loader to resolve a custom class with
   * @return the fully-qualified strategy class name
   * @throws ValidationException if the value is blank, names no loadable class, or names a class
   *     that is not a {@link SubjectNameStrategy}
   */
  public static String validatedClassName(String optionKey, String value, ClassLoader classLoader) {
    if (value.trim().isEmpty()) {
      throw new ValidationException(
          "'" + optionKey + "' must not be empty. " + allowedValues() + ".");
    }
    String className = className(value);
    Class<?> clazz;
    try {
      clazz = Class.forName(className, false, classLoader);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new ValidationException(
          "'"
              + optionKey
              + "' is set to '"
              + value
              + "', which is neither a known strategy nor a class on the classpath. "
              + allowedValues()
              + ".",
          e);
    }
    if (!SubjectNameStrategy.class.isAssignableFrom(clazz)) {
      throw new ValidationException(
          "'"
              + optionKey
              + "' is set to '"
              + value
              + "', but "
              + className
              + " does not implement "
              + SubjectNameStrategy.class.getName()
              + ". "
              + allowedValues()
              + ".");
    }
    return className;
  }

  private static String allowedValues() {
    return "Use 'topic', 'record', 'topic-record' (case-insensitive) or the fully-qualified name "
        + "of a class implementing "
        + SubjectNameStrategy.class.getName();
  }
}
