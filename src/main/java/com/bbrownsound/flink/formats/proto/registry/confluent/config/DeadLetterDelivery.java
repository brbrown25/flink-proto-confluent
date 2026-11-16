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
 * How a record that failed to deserialize is delivered to the dead-letter topic.
 *
 * <p>This is the type of the {@code dead-letter.delivery} option. Like {@link OnDeserializeError}
 * it is an enum, so that an unknown value (for example the typo {@code sink}) is rejected when a
 * query that uses the table is planned instead of silently selecting the weaker {@link #ASYNC}.
 */
public enum DeadLetterDelivery {
  /**
   * Hand the record to the producer and carry on. A failed send is logged and counted, but the
   * source may already have moved past the record. This is the default.
   */
  ASYNC,

  /**
   * Wait for the broker to acknowledge the record before the source moves on; a failed send fails
   * the task instead of dropping the record.
   */
  SYNC;

  /**
   * Reads and validates the {@code dead-letter.delivery} option (or its {@code
   * dead_letter.delivery} fallback key).
   *
   * @param formatOptions the format options
   * @return the configured delivery mode, {@link #ASYNC} when the option is unset
   * @throws ValidationException if the configured value is not one of the allowed values
   */
  public static DeadLetterDelivery fromOptions(ReadableConfig formatOptions) {
    try {
      return formatOptions.get(ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY);
    } catch (RuntimeException e) {
      throw invalid(rawValue(formatOptions), e);
    }
  }

  /**
   * The allowed values as they are written in table options.
   *
   * @return a comma-separated, lower-case list of the allowed values
   */
  public static String allowedValues() {
    return Arrays.stream(values())
        .map(DeadLetterDelivery::toString)
        .collect(Collectors.joining(", "));
  }

  private static String rawValue(ReadableConfig formatOptions) {
    // Untyped view of the option, built from the option itself so the keys cannot drift apart. It
    // is used only to quote the offending raw value in the error message.
    ConfigOption<DeadLetterDelivery> typed = ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY;
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
            value, ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY.key(), allowedValues()),
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
