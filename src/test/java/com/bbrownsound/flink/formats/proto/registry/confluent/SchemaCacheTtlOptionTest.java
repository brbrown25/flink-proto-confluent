package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.ProtoRowDataSerializationSchema;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The {@code schema-cache.ttl} option (issue #172) from its definition, through the factory and
 * {@link ProtoConfluentFormatConfig}, to the runtime encoder. What the TTL does at runtime is
 * covered by the tests of the serializer.
 */
class SchemaCacheTtlOptionTest {

  private final RegistryProtoFormatFactory factory = new RegistryProtoFormatFactory();

  private static Map<String, String> requiredOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("url", "http://localhost:8081");
    options.put("topic", "orders");
    return options;
  }

  private static Configuration optionsWith(String key, String value) {
    Map<String, String> options = requiredOptions();
    options.put(key, value);
    return Configuration.fromMap(options);
  }

  // --- definition ------------------------------------------------------------------------------

  @Test
  void option_isNamedLikeTheOtherOptionsAndHasAnUnderscoreFallback() {
    assertEquals("schema-cache.ttl", ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL.key());
    assertTrue(
        Stream.of(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL.fallbackKeys())
            .flatMap(keys -> java.util.stream.StreamSupport.stream(keys.spliterator(), false))
            .map(FallbackKey::getKey)
            .anyMatch("schema_cache.ttl"::equals));
  }

  @Test
  void option_hasNoDefault_soAnUnsetOptionKeepsTheSchemaPinned() {
    assertFalse(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL.hasDefaultValue());
    assertNull(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL.defaultValue());
  }

  @Test
  void factory_listsTheOptionAsOptionalAndForwarded() {
    assertTrue(factory.optionalOptions().contains(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL));
    assertTrue(factory.forwardOptions().contains(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL));
    assertFalse(factory.requiredOptions().contains(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL));
  }

  // --- config ----------------------------------------------------------------------------------

  @Test
  void config_defaultsToNoTtl() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(Configuration.fromMap(requiredOptions()));

    assertNull(config.schemaCacheTtl);
  }

  @Test
  void config_fromExplicitParameters_hasNoTtl() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://localhost:8081", "orders", false, Map.of());

    assertNull(config.schemaCacheTtl);
  }

  @Test
  void config_readsATypedValue() {
    Configuration options = Configuration.fromMap(requiredOptions());
    options.set(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL, Duration.ofMinutes(5));

    assertEquals(Duration.ofMinutes(5), new ProtoConfluentFormatConfig(options).schemaCacheTtl);
  }

  /** The values a table DDL would carry, in Flink's duration syntax. */
  @ParameterizedTest(name = "''{0}'' -> {1}")
  @CsvSource({
    "5 min, PT5M",
    "30 s, PT30S",
    "30s, PT30S",
    "1 h, PT1H",
    "500 ms, PT0.5S",
    "1 d, PT24H"
  })
  void config_parsesADdlString(String value, Duration expected) {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(optionsWith("schema-cache.ttl", value));

    assertEquals(expected, config.schemaCacheTtl);
  }

  @Test
  void config_readsTheUnderscoreFallbackKey() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(optionsWith("schema_cache.ttl", "2 min"));

    assertEquals(Duration.ofMinutes(2), config.schemaCacheTtl);
  }

  @Test
  void config_prefersTheHyphenatedKeyOverTheFallback() {
    Map<String, String> options = requiredOptions();
    options.put("schema_cache.ttl", "2 min");
    options.put("schema-cache.ttl", "7 min");

    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(Configuration.fromMap(options));

    assertEquals(Duration.ofMinutes(7), config.schemaCacheTtl);
  }

  @Test
  void config_doesNotLeakTheOptionIntoTheRegistryClientProperties() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(optionsWith("schema-cache.ttl", "5 min"));

    assertTrue(
        config.getProperties().keySet().stream().noneMatch(key -> key.contains("cache")),
        config.getProperties().toString());
  }

  static Stream<Duration> nonPositiveDurations() {
    return Stream.of(Duration.ZERO, Duration.ofMillis(-1), Duration.ofMinutes(-5));
  }

  @ParameterizedTest
  @MethodSource("nonPositiveDurations")
  void createEncodingFormat_rejectsANonPositiveDuration(Duration ttl) {
    Configuration options = Configuration.fromMap(requiredOptions());
    options.set(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL, ttl);

    ValidationException thrown =
        assertThrows(ValidationException.class, () -> factory.createEncodingFormat(null, options));

    assertTrue(thrown.getMessage().contains("schema-cache.ttl"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("positive"), thrown.getMessage());
  }

  /** The option is sink-only, but a bad value must not wait for the first INSERT to be caught. */
  @ParameterizedTest
  @MethodSource("nonPositiveDurations")
  void createDecodingFormat_rejectsANonPositiveDuration(Duration ttl) {
    Configuration options = Configuration.fromMap(requiredOptions());
    options.set(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL, ttl);

    ValidationException thrown =
        assertThrows(ValidationException.class, () -> factory.createDecodingFormat(null, options));

    assertTrue(thrown.getMessage().contains("schema-cache.ttl"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("positive"), thrown.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0 s", "0", "0 ms"})
  void createEncodingFormat_rejectsAZeroDdlString(String value) {
    Configuration options = optionsWith("schema-cache.ttl", value);

    assertThrows(ValidationException.class, () -> factory.createEncodingFormat(null, options));
  }

  /**
   * A config built directly, without the factory, carries the value as given; the serializer
   * rejects it when the sink opens.
   */
  @Test
  void config_builtWithoutTheFactory_leavesValidationToTheSerializer() {
    Configuration options = optionsWith("schema-cache.ttl", "0 s");

    assertEquals(Duration.ZERO, new ProtoConfluentFormatConfig(options).schemaCacheTtl);
  }

  /** Flink itself refuses these before the format sees a duration. */
  @ParameterizedTest
  @ValueSource(strings = {"-5 min", "soon", "5 fortnights", ""})
  void config_rejectsAnUnparseableDdlString(String value) {
    Configuration options = optionsWith("schema-cache.ttl", value);

    assertThrows(RuntimeException.class, () -> new ProtoConfluentFormatConfig(options));
  }

  @Test
  void config_carriesTheTtlThroughJavaSerialization() throws Exception {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(optionsWith("schema-cache.ttl", "5 min"));

    ProtoConfluentFormatConfig copy = InstantiationUtil.clone(config);

    assertEquals(Duration.ofMinutes(5), copy.schemaCacheTtl);
    assertEquals(config.getProperties(), copy.getProperties());
  }

  @Test
  void config_withoutTtl_survivesJavaSerialization() throws Exception {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(Configuration.fromMap(requiredOptions()));

    assertNull(InstantiationUtil.clone(config).schemaCacheTtl);
  }

  // --- factory and encoding format ---------------------------------------------------------------

  @Test
  void createEncodingFormat_acceptsTheOption_andTheEncoderIsSerializable() throws Exception {
    EncodingFormat<SerializationSchema<RowData>> format =
        factory.createEncodingFormat(null, optionsWith("schema-cache.ttl", "5 min"));
    assertInstanceOf(ProtoEncodingFormat.class, format);

    SerializationSchema<RowData> encoder =
        format.createRuntimeEncoder(
            (DynamicTableSink.Context) null,
            DataTypes.ROW(
                DataTypes.FIELD("content", DataTypes.STRING()),
                DataTypes.FIELD("date_time", DataTypes.STRING())));

    assertInstanceOf(ProtoRowDataSerializationSchema.class, encoder);
    assertNotNull(InstantiationUtil.clone(encoder));
  }

  @Test
  void createEncodingFormat_withTheFallbackKey_isAccepted() {
    assertNotNull(factory.createEncodingFormat(null, optionsWith("schema_cache.ttl", "5 min")));
  }

  /** The option only matters to a sink, but a table used as both must not fail to read. */
  @Test
  void createDecodingFormat_acceptsTheOption() {
    assertNotNull(factory.createDecodingFormat(null, optionsWith("schema-cache.ttl", "5 min")));
  }
}
