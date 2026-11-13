package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

/**
 * What the source does with a record that cannot be deserialized.
 *
 * <p>This is the type of the {@code on-deserialize-error} option. It is an enum, rather than a
 * free-form string, so that an unknown value (for example the typo {@code fial}) is rejected when
 * a query that uses the table is planned instead of silently selecting {@link #SKIP} and dropping poison records.
 */
public enum OnDeserializeError {
  /** Throw, failing the task. This is the default. */
  FAIL,

  /** Log and count the record, route it to the dead-letter topic if one is set, then drop it. */
  SKIP;

  /**
   * Reads and validates the {@code on-deserialize-error} option (or its {@code
   * on_deserialize_error} fallback key).
   *
   * @param formatOptions the format options
   * @return the configured behavior, {@link #FAIL} when the option is unset
   * @throws ValidationException if the configured value is not one of the allowed values
   */
  public static OnDeserializeError fromOptions(ReadableConfig formatOptions) {
    try {
      return formatOptions.get(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR);
    } catch (RuntimeException e) {
      throw invalid(rawValue(formatOptions), e);
    }
  }

  /**
   * Parses a value case-insensitively.
   *
   * @param value the textual value, e.g. {@code "fail"} or {@code "SKIP"}
   * @return the matching constant
   * @throws ValidationException if the value is null or not one of the allowed values
   */
  public static OnDeserializeError fromString(String value) {
    if (value != null) {
      for (OnDeserializeError candidate : values()) {
        if (candidate.name().equalsIgnoreCase(value)) {
          return candidate;
        }
      }
    }
    throw invalid(value, null);
  }

  /**
   * The allowed values as they are written in table options.
   *
   * @return a comma-separated, lower-case list of the allowed values
   */
  public static String allowedValues() {
    return Arrays.stream(values())
        .map(OnDeserializeError::toString)
        .collect(Collectors.joining(", "));
  }

  private static String rawValue(ReadableConfig formatOptions) {
    // Untyped view of the option, built from the option itself so the keys cannot drift apart. It
    // is used only to quote the offending raw value in the error message.
    ConfigOption<OnDeserializeError> typed = ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR;
    String[] fallbackKeys =
        StreamSupport.stream(typed.fallbackKeys().spliterator(), false)
            .map(FallbackKey::getKey)
            .toArray(String[]::new);
    ConfigOption<String> raw =
        ConfigOptions.key(typed.key())
            .stringType()
            .noDefaultValue()
            .withFallbackKeys(fallbackKeys);
    return formatOptions.getOptional(raw).orElse(null);
  }

  private static ValidationException invalid(String value, Throwable cause) {
    return new ValidationException(
        String.format(
            "Invalid value '%s' for option '%s'. Allowed values (case-insensitive): %s.",
            value, ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR.key(), allowedValues()),
        cause);
  }

  /**
   * The value as it is written in table options.
   *
   * @return the lower-case name
   */
  @Override
  public String toString() {
    return name().toLowerCase(Locale.ROOT);
  }
}
