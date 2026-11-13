package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers GitHub issue #170: {@code on-deserialize-error} is an enum option, so an unknown value
 * (for example the typo {@code fial}) is rejected with a {@link ValidationException} that lists the
 * allowed values, instead of silently selecting {@code skip} and dropping poison records.
 *
 * <p>Every case is exercised through the two real entry points: {@link
 * RegistryProtoFormatFactory#createDecodingFormat} called directly, and Flink's {@link FactoryUtil}
 * helper, which discovers the factory through the service loader from a table's {@code format}
 * option exactly as a connector does.
 */
@DisplayName("on-deserialize-error value validation (issue #170)")
class OnDeserializeErrorValidationTest {

  private static final String PRIMARY_KEY = "on-deserialize-error";
  private static final String FALLBACK_KEY = "on_deserialize_error";
  private static final byte[] POISON = new byte[] {0, 1, 2, 3, 4};

  private final RegistryProtoFormatFactory factory = new RegistryProtoFormatFactory();

  // ---------------------------------------------------------------------------------------------
  // Rejected values
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] ''{0}''")
  @ValueSource(
      strings = {
        "fial", "", "skipp", "bogus", "ignore", "drop", "true", "false", "0", "fail,skip", " skip",
        "fail ", "f", "FAILL", "SKIP_ALL", "null"
      })
  void createDecodingFormat_unknownValue_primaryKey_isRejected(String value) {
    assertRejected(
        value, () -> factory.createDecodingFormat(null, formatOptions(PRIMARY_KEY, value)));
  }

  @ParameterizedTest(name = "[{index}] ''{0}''")
  @ValueSource(strings = {"fial", "", "skipp", "bogus", "ignore", "true", " skip", "FAILL"})
  void createDecodingFormat_unknownValue_fallbackKey_isRejected(String value) {
    assertRejected(
        value, () -> factory.createDecodingFormat(null, formatOptions(FALLBACK_KEY, value)));
  }

  @ParameterizedTest(name = "[{index}] ''{0}''")
  @ValueSource(strings = {"fial", "", "skipp"})
  void createEncodingFormat_unknownValue_isRejected(String value) {
    // The option is declared by the factory for both directions, so a bad value fails a sink too.
    assertRejected(
        value, () -> factory.createEncodingFormat(null, formatOptions(PRIMARY_KEY, value)));
    assertRejected(
        value, () -> factory.createEncodingFormat(null, formatOptions(FALLBACK_KEY, value)));
  }

  @ParameterizedTest(name = "[{index}] {0} = ''{1}''")
  @CsvSource({
    "on-deserialize-error,fial",
    "on-deserialize-error,''",
    "on-deserialize-error,skipp",
    "on_deserialize_error,fial",
    "on_deserialize_error,''",
    "on_deserialize_error,skipp"
  })
  void factoryUtil_unknownValue_isRejectedWithAllowedValues(String key, String value) {
    ValidationException e =
        assertThrows(ValidationException.class, () -> discoverDecodingFormat(key, value));
    // FactoryUtil wraps whatever the format factory throws ("Error creating scan format ...");
    // the factory's own ValidationException, which names the allowed values, is the cause.
    assertTrue(e.getMessage().contains("proto-confluent"), e.getMessage());
    ValidationException cause = assertInstanceOf(ValidationException.class, e.getCause());
    assertListsAllowedValues(cause, value);
  }

  @Test
  void unknownValue_isRejectedEvenWhenPrimaryKeyIsValidOnlyInFallback() {
    // The primary key wins over the fallback key, so a typo there is not masked by a valid
    // fallback value.
    Configuration options = formatOptions(PRIMARY_KEY, "fial");
    options.setString(FALLBACK_KEY, "fail");
    assertRejected("fial", () -> factory.createDecodingFormat(null, options));
  }

  @Test
  void unknownValue_withDeadLetterTopic_isStillRejected() {
    Configuration options = formatOptions(PRIMARY_KEY, "skipp");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    options.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
        Map.of("bootstrap.servers", "localhost:9092"));
    assertRejected("skipp", () -> factory.createDecodingFormat(null, options));
  }

  @Test
  void unknownValue_messageNamesTheOptionAndKeepsFlinkCause() {
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> factory.createDecodingFormat(null, formatOptions(PRIMARY_KEY, "fial")));
    assertEquals(
        "Invalid value 'fial' for option 'on-deserialize-error'. "
            + "Allowed values (case-insensitive): fail, skip.",
        e.getMessage());
    assertNotNull(e.getCause(), "Flink's own parse failure is kept as the cause");
  }

  // ---------------------------------------------------------------------------------------------
  // Accepted values
  // ---------------------------------------------------------------------------------------------

  @ParameterizedTest(name = "[{index}] {0} = ''{1}'' -> {2}")
  @CsvSource({
    "on-deserialize-error,fail,FAIL",
    "on-deserialize-error,FAIL,FAIL",
    "on-deserialize-error,Fail,FAIL",
    "on-deserialize-error,fAiL,FAIL",
    "on-deserialize-error,skip,SKIP",
    "on-deserialize-error,SKIP,SKIP",
    "on-deserialize-error,Skip,SKIP",
    "on-deserialize-error,sKiP,SKIP",
    "on_deserialize_error,fail,FAIL",
    "on_deserialize_error,FAIL,FAIL",
    "on_deserialize_error,Fail,FAIL",
    "on_deserialize_error,skip,SKIP",
    "on_deserialize_error,SKIP,SKIP",
    "on_deserialize_error,Skip,SKIP"
  })
  void knownValue_isAcceptedCaseInsensitively_andDrivesRuntimeBehavior(
      String key, String value, OnDeserializeError expected) throws Exception {
    // Direct factory call.
    assertRuntimeBehavior(factory.createDecodingFormat(null, formatOptions(key, value)), expected);
    // Service-loader discovery through FactoryUtil, as a connector does it.
    assertRuntimeBehavior(discoverDecodingFormat(key, value), expected);
    // A sink carrying the option is accepted too.
    assertNotNull(factory.createEncodingFormat(null, formatOptions(key, value)));
  }

  @Test
  void unset_defaultsToFail_throughBothEntryPoints() throws Exception {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "test-topic");
    assertEquals(
        OnDeserializeError.FAIL, options.get(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR));
    assertRuntimeBehavior(factory.createDecodingFormat(null, options), OnDeserializeError.FAIL);
    assertRuntimeBehavior(discoverDecodingFormat(null, null), OnDeserializeError.FAIL);
  }

  @Test
  void typedEnumValue_isAccepted() throws Exception {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "test-topic");
    options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, OnDeserializeError.SKIP);
    assertRuntimeBehavior(factory.createDecodingFormat(null, options), OnDeserializeError.SKIP);
  }

  @Test
  void skipWithDeadLetterTopic_isAccepted_throughFactoryUtil() {
    Map<String, String> extra = new HashMap<>();
    extra.put("proto-confluent.dead-letter-topic", "dlq");
    extra.put("proto-confluent.dead-letter.properties", "security.protocol:SSL");
    assertNotNull(discoverDecodingFormat(PRIMARY_KEY, "skip", extra));
  }

  @Test
  void option_isAnEnumOptionWithFailDefaultAndFallbackKey() {
    ConfigOption<OnDeserializeError> option = ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR;
    assertEquals(PRIMARY_KEY, option.key());
    assertEquals(OnDeserializeError.FAIL, option.defaultValue());
    Set<String> fallbackKeys = new HashSet<>();
    option.fallbackKeys().forEach(k -> fallbackKeys.add(k.getKey()));
    assertEquals(Set.of(FALLBACK_KEY), fallbackKeys);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static Configuration formatOptions(String key, String value) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "test-topic");
    // Raw text, the way a table option arrives from DDL.
    options.setString(key, value);
    return options;
  }

  private static void assertRejected(String value, Runnable action) {
    ValidationException e = assertThrows(ValidationException.class, action::run);
    assertListsAllowedValues(e, value);
  }

  private static void assertListsAllowedValues(ValidationException e, String value) {
    String message = e.getMessage();
    assertTrue(message.contains("'on-deserialize-error'"), message);
    assertTrue(message.contains("'" + value + "'"), message);
    for (OnDeserializeError allowed : OnDeserializeError.values()) {
      assertTrue(message.contains(allowed.toString()), message);
    }
    assertTrue(message.contains("fail, skip"), message);
  }

  /**
   * Builds the runtime decoder from the format, Java-serializes it the way Flink ships it to a
   * task, and feeds the copy a poison record.
   */
  private static void assertRuntimeBehavior(
      DecodingFormat<DeserializationSchema<RowData>> format, OnDeserializeError expected)
      throws Exception {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    DeserializationSchema<RowData> schema =
        InstantiationUtil.clone(
            format.createRuntimeDecoder(
                new StubSourceContext(), TypeConversions.fromLogicalToDataType(rowType)));

    MetricListener listener = new MetricListener();
    schema.open(
        new DeserializationSchema.InitializationContext() {
          @Override
          public MetricGroup getMetricGroup() {
            return listener.getMetricGroup();
          }

          @Override
          public UserCodeClassLoader getUserCodeClassLoader() {
            return SimpleUserCodeClassLoader.create(
                OnDeserializeErrorValidationTest.class.getClassLoader());
          }
        });

    if (expected == OnDeserializeError.SKIP) {
      assertNull(schema.deserialize(POISON), "SKIP must drop the poison record");
    } else {
      assertThrows(
          IOException.class, () -> schema.deserialize(POISON), "FAIL must fail on poison");
    }
    // Counted either way.
    assertEquals(1L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  private static DecodingFormat<DeserializationSchema<RowData>> discoverDecodingFormat(
      String key, String value) {
    return discoverDecodingFormat(key, value, Collections.emptyMap());
  }

  private static DecodingFormat<DeserializationSchema<RowData>> discoverDecodingFormat(
      String key, String value, Map<String, String> extraOptions) {
    Map<String, String> options = new HashMap<>();
    options.put("connector", StubSourceFactory.IDENTIFIER);
    options.put("format", RegistryProtoFormatFactory.IDENTIFIER);
    options.put("proto-confluent.url", "http://localhost:8081");
    options.put("proto-confluent.topic", "test-topic");
    if (key != null) {
      options.put("proto-confluent." + key, value);
    }
    options.putAll(extraOptions);

    ResolvedCatalogTable table =
        new ResolvedCatalogTable(
            CatalogTable.newBuilder()
                .schema(Schema.newBuilder().column("content", DataTypes.STRING()).build())
                .options(options)
                .build(),
            ResolvedSchema.of(Column.physical("content", DataTypes.STRING())));
    DynamicTableFactory.Context context =
        new FactoryUtil.DefaultDynamicTableContext(
            ObjectIdentifier.of("cat", "db", "t"),
            table,
            Collections.emptyMap(),
            new Configuration(),
            OnDeserializeErrorValidationTest.class.getClassLoader(),
            false);

    FactoryUtil.TableFactoryHelper helper =
        FactoryUtil.createTableFactoryHelper(new StubSourceFactory(), context);
    DecodingFormat<DeserializationSchema<RowData>> format =
        helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
    // Also proves that every 'proto-confluent.*' key above is a declared option of the factory.
    helper.validate();
    return format;
  }

  @Test
  void factoryUtil_discoversTheFormatForBothDirections() {
    // Guards the helper above: the format really is resolved through the service loader.
    assertInstanceOf(
        RegistryProtoFormatFactory.class,
        FactoryUtil.discoverFactory(
            getClass().getClassLoader(),
            DeserializationFormatFactory.class,
            RegistryProtoFormatFactory.IDENTIFIER));
    assertInstanceOf(
        RegistryProtoFormatFactory.class,
        FactoryUtil.discoverFactory(
            getClass().getClassLoader(),
            SerializationFormatFactory.class,
            RegistryProtoFormatFactory.IDENTIFIER));
  }

  /** Minimal connector factory: only there so {@link FactoryUtil} has a table factory to help. */
  private static final class StubSourceFactory implements DynamicTableSourceFactory {
    static final String IDENTIFIER = "stub";

    @Override
    public String factoryIdentifier() {
      return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
      return Set.of(FactoryUtil.FORMAT);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
      return Collections.emptySet();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
      throw new UnsupportedOperationException("not used");
    }
  }

  /** The runtime decoder only asks the context for type information, which it merely stores. */
  private static final class StubSourceContext implements DynamicTableSource.Context {
    @Override
    public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
      return null;
    }

    @Override
    public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
      return null;
    }

    @Override
    public DynamicTableSource.DataStructureConverter createDataStructureConverter(
        DataType producedDataType) {
      throw new UnsupportedOperationException("not used");
    }
  }
}
