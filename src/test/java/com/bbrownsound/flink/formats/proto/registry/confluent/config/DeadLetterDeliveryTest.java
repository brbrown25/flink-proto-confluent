package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Covers GitHub issue #171: the {@code dead-letter.delivery} enum option and its validation. */
@DisplayName("DeadLetterDelivery")
class DeadLetterDeliveryTest {

  private static final String PRIMARY_KEY = "dead-letter.delivery";
  private static final String FALLBACK_KEY = "dead_letter.delivery";

  private static Configuration options(String key, String value) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    if (key != null) {
      // Raw text, the way a table option arrives from DDL.
      options.setString(key, value);
    }
    return options;
  }

  @Test
  void values_areExactlyAsyncAndSync() {
    assertArrayEquals(
        new DeadLetterDelivery[] {DeadLetterDelivery.ASYNC, DeadLetterDelivery.SYNC},
        DeadLetterDelivery.values());
  }

  @Test
  void toString_isTheLowerCaseOptionValue() {
    assertEquals("async", DeadLetterDelivery.ASYNC.toString());
    assertEquals("sync", DeadLetterDelivery.SYNC.toString());
    assertEquals("async, sync", DeadLetterDelivery.allowedValues());
  }

  @Test
  void option_isAnEnumOptionWithAsyncDefaultAndFallbackKey() {
    ConfigOption<DeadLetterDelivery> option = ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY;
    assertEquals(PRIMARY_KEY, option.key());
    assertEquals(DeadLetterDelivery.ASYNC, option.defaultValue());
    Set<String> fallbackKeys = new HashSet<>();
    option.fallbackKeys().forEach(k -> fallbackKeys.add(k.getKey()));
    assertEquals(Set.of(FALLBACK_KEY), fallbackKeys);
  }

  @Test
  void fromOptions_unset_defaultsToAsync() {
    assertSame(DeadLetterDelivery.ASYNC, DeadLetterDelivery.fromOptions(options(null, null)));
    assertSame(
        DeadLetterDelivery.ASYNC,
        new ProtoConfluentFormatConfig(options(null, null)).deadLetterDelivery);
    assertSame(
        DeadLetterDelivery.ASYNC,
        new ProtoConfluentFormatConfig("http://sr:8081", "events", false, Map.of())
            .deadLetterDelivery);
  }

  @ParameterizedTest(name = "[{index}] {0} = ''{1}'' -> {2}")
  @CsvSource({
    "dead-letter.delivery,async,ASYNC",
    "dead-letter.delivery,ASYNC,ASYNC",
    "dead-letter.delivery,Async,ASYNC",
    "dead-letter.delivery,sync,SYNC",
    "dead-letter.delivery,SYNC,SYNC",
    "dead-letter.delivery,Sync,SYNC",
    "dead_letter.delivery,async,ASYNC",
    "dead_letter.delivery,sync,SYNC",
    "dead_letter.delivery,sYNC,SYNC"
  })
  void fromOptions_isCaseInsensitive_underBothKeys(
      String key, String value, DeadLetterDelivery expected) {
    assertSame(expected, DeadLetterDelivery.fromOptions(options(key, value)));
    assertSame(expected, new ProtoConfluentFormatConfig(options(key, value)).deadLetterDelivery);
  }

  @ParameterizedTest(name = "[{index}] {0} = ''{1}''")
  @CsvSource({
    "dead-letter.delivery,sink",
    "dead-letter.delivery,''",
    "dead-letter.delivery,synchronous",
    "dead-letter.delivery,true",
    "dead-letter.delivery,' sync'",
    "dead-letter.delivery,'async,sync'",
    "dead_letter.delivery,sink",
    "dead_letter.delivery,''"
  })
  void fromOptions_unknownValue_throwsValidationExceptionListingAllowedValues(
      String key, String value) {
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> DeadLetterDelivery.fromOptions(options(key, value)));
    assertTrue(e.getMessage().contains("'" + value + "'"), e.getMessage());
    assertTrue(e.getMessage().contains("'dead-letter.delivery'"), e.getMessage());
    assertTrue(e.getMessage().contains("async, sync"), e.getMessage());
    assertNotNull(e.getCause(), "Flink's own parse error is kept as the cause");

    // Building the config is what a table does: an unknown value never means "async".
    assertThrows(
        ValidationException.class, () -> new ProtoConfluentFormatConfig(options(key, value)));
  }

  @Test
  void deadLetterDelivery_survivesJavaSerialization() throws Exception {
    // The config is Java-serialized into the job graph inside the deserialization schema.
    ProtoConfluentFormatConfig copy =
        InstantiationUtil.clone(new ProtoConfluentFormatConfig(options(PRIMARY_KEY, "sync")));
    assertSame(DeadLetterDelivery.SYNC, copy.deadLetterDelivery);
  }

  @Test
  void defaultDeadLetterBootstrapServers_onlyFillsAGap_andOnlyWithADeadLetterTopic() {
    // No dead-letter topic: nothing to default.
    assertTrue(
        new ProtoConfluentFormatConfig(options(null, null), "table-kafka:9092")
            .getDeadLetterProperties()
            .isEmpty());

    Configuration withTopic = options(null, null);
    withTopic.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    assertEquals(
        Map.of("bootstrap.servers", "table-kafka:9092"),
        new ProtoConfluentFormatConfig(withTopic, "table-kafka:9092").getDeadLetterProperties());
    assertTrue(new ProtoConfluentFormatConfig(withTopic, null).getDeadLetterProperties().isEmpty());

    // Brokers named in 'dead-letter.properties' always win.
    withTopic.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
        Map.of("bootstrap.servers", "dlq-kafka:9092"));
    assertEquals(
        Map.of("bootstrap.servers", "dlq-kafka:9092"),
        new ProtoConfluentFormatConfig(withTopic, "table-kafka:9092").getDeadLetterProperties());
  }
}
