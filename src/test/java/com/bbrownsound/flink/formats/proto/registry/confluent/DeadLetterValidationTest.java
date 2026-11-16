package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers GitHub issue #171 at the format factory: a {@code dead-letter-topic} needs brokers (its
 * own {@code bootstrap.servers}, or failing that the table's {@code
 * properties.bootstrap.servers}), and {@code dead-letter.delivery} only accepts {@code async} and
 * {@code sync}. Both are rejected with a {@link ValidationException} when the table is planned
 * instead of degrading at runtime.
 */
@DisplayName("dead-letter option validation (issue #171)")
class DeadLetterValidationTest {

  private final RegistryProtoFormatFactory factory = new RegistryProtoFormatFactory();

  private static Configuration formatOptions() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "test-topic");
    return options;
  }

  private static Configuration deadLetterOptions(Map<String, String> deadLetterProperties) {
    Configuration options = formatOptions();
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    if (deadLetterProperties != null) {
      options.set(ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES, deadLetterProperties);
    }
    return options;
  }

  /** The context a connector hands the format factory: it exposes all of the table's options. */
  private static DynamicTableFactory.Context tableContext(Map<String, String> tableOptions) {
    ResolvedCatalogTable table =
        new ResolvedCatalogTable(
            CatalogTable.newBuilder()
                .schema(Schema.newBuilder().column("content", DataTypes.STRING()).build())
                .options(tableOptions)
                .build(),
            ResolvedSchema.of(Column.physical("content", DataTypes.STRING())));
    return new FactoryUtil.DefaultDynamicTableContext(
        ObjectIdentifier.of("cat", "db", "t"),
        table,
        Collections.emptyMap(),
        new Configuration(),
        DeadLetterValidationTest.class.getClassLoader(),
        false);
  }

  private static ProtoConfluentFormatConfig configOf(Object format) throws Exception {
    Field field = format.getClass().getDeclaredField("formatConfig");
    field.setAccessible(true);
    return (ProtoConfluentFormatConfig) field.get(format);
  }

  private static void assertNamesTheMissingBrokers(ValidationException e) {
    String message = e.getMessage();
    assertTrue(message.contains("'dead-letter-topic' is set to 'dlq'"), message);
    assertTrue(message.contains("'dead-letter.properties' has no 'bootstrap.servers'"), message);
    assertTrue(message.contains("'properties.bootstrap.servers'"), message);
  }

  // -------------------------------------------------------------------------------------------
  // dead-letter-topic needs brokers
  // -------------------------------------------------------------------------------------------

  @Test
  void topicWithoutAnyBrokers_isRejected_forSourceAndSink() {
    Configuration options = deadLetterOptions(null);
    // No context at all (the factory called directly) and a table without Kafka's own brokers.
    DynamicTableFactory.Context noBrokers = tableContext(Map.of("connector", "stub"));

    assertNamesTheMissingBrokers(
        assertThrows(
            ValidationException.class, () -> factory.createDecodingFormat(null, options)));
    assertNamesTheMissingBrokers(
        assertThrows(
            ValidationException.class, () -> factory.createEncodingFormat(null, options)));
    assertNamesTheMissingBrokers(
        assertThrows(
            ValidationException.class, () -> factory.createDecodingFormat(noBrokers, options)));
    assertNamesTheMissingBrokers(
        assertThrows(
            ValidationException.class, () -> factory.createEncodingFormat(noBrokers, options)));
  }

  @Test
  void topicWithPropertiesButNoBrokers_isRejected() {
    Configuration options = deadLetterOptions(Map.of("security.protocol", "SSL"));
    assertNamesTheMissingBrokers(
        assertThrows(
            ValidationException.class, () -> factory.createDecodingFormat(null, options)));
  }

  @Test
  void topicWithoutBrokers_fallsBackToTheTablesBootstrapServers() throws Exception {
    Configuration options = deadLetterOptions(Map.of("security.protocol", "SSL"));
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put("connector", "kafka");
    tableOptions.put("properties.bootstrap.servers", "table-kafka:9092");
    DynamicTableFactory.Context context = tableContext(tableOptions);

    ProtoConfluentFormatConfig source = configOf(factory.createDecodingFormat(context, options));
    assertEquals("table-kafka:9092", source.getDeadLetterProperties().get("bootstrap.servers"));
    // Only the brokers are inherited; the rest of 'dead-letter.properties' is kept as given.
    assertEquals("SSL", source.getDeadLetterProperties().get("security.protocol"));
    assertEquals(2, source.getDeadLetterProperties().size());

    ProtoConfluentFormatConfig sink = configOf(factory.createEncodingFormat(context, options));
    assertEquals("table-kafka:9092", sink.getDeadLetterProperties().get("bootstrap.servers"));
  }

  @Test
  void explicitBrokers_winOverTheTablesBootstrapServers() throws Exception {
    Configuration options = deadLetterOptions(Map.of("bootstrap.servers", "dlq-kafka:9092"));
    DynamicTableFactory.Context context =
        tableContext(Map.of("properties.bootstrap.servers", "table-kafka:9092"));

    ProtoConfluentFormatConfig config = configOf(factory.createDecodingFormat(context, options));
    assertEquals("dlq-kafka:9092", config.getDeadLetterProperties().get("bootstrap.servers"));
    // And they are enough on their own, with no table context to fall back to.
    assertNotNull(factory.createDecodingFormat(null, options));
  }

  @Test
  void noDeadLetterTopic_needsNoBrokers() throws Exception {
    ProtoConfluentFormatConfig config =
        configOf(
            factory.createDecodingFormat(
                tableContext(Map.of("properties.bootstrap.servers", "table-kafka:9092")),
                formatOptions()));
    assertTrue(
        config.getDeadLetterProperties().isEmpty(),
        "Nothing is inherited from the table unless a dead-letter topic is set");
  }

  // -------------------------------------------------------------------------------------------
  // dead-letter.delivery
  // -------------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] ''{0}''")
  @ValueSource(strings = {"sink", "", "synchronous", "true", "at-least-once"})
  void unknownDelivery_isRejectedWithAllowedValues_forSourceAndSink(String value) {
    Configuration options = formatOptions();
    options.setString("dead-letter.delivery", value);

    for (ValidationException e :
        new ValidationException[] {
          assertThrows(
              ValidationException.class, () -> factory.createDecodingFormat(null, options)),
          assertThrows(
              ValidationException.class, () -> factory.createEncodingFormat(null, options))
        }) {
      assertTrue(e.getMessage().contains("'dead-letter.delivery'"), e.getMessage());
      assertTrue(e.getMessage().contains("'" + value + "'"), e.getMessage());
      assertTrue(e.getMessage().contains("async, sync"), e.getMessage());
    }
  }

  @ParameterizedTest(name = "[{index}] ''{0}''")
  @ValueSource(strings = {"async", "sync", "SYNC", "Async"})
  void knownDelivery_isAccepted_andReachesTheConfig(String value) throws Exception {
    Configuration options = deadLetterOptions(Map.of("bootstrap.servers", "dlq-kafka:9092"));
    options.setString("dead_letter.delivery", value);

    ProtoConfluentFormatConfig config = configOf(factory.createDecodingFormat(null, options));
    assertEquals(value.toLowerCase(java.util.Locale.ROOT), config.deadLetterDelivery.toString());
    assertNotNull(factory.createEncodingFormat(null, options));
  }

  @Test
  void delivery_isADeclaredAndForwardedOption() {
    assertTrue(factory.optionalOptions().contains(ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY));
    assertTrue(factory.forwardOptions().contains(ProtoConfluentFormatOptions.DEAD_LETTER_DELIVERY));
  }
}
