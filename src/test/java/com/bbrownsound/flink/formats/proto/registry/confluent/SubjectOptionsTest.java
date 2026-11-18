package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.subject.FixedSubjectNameStrategy;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.lang.reflect.Field;
import java.util.Collections;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Covers GitHub issue #176 at the format factory: {@code topic} is optional when the table's own
 * Kafka {@code topic} names one topic or {@code subject} is set, {@code subject-name-strategy}
 * routes a strategy to the key or value serde property, and both typed options win over the raw
 * {@code properties} passthrough.
 */
@DisplayName("subject and subject-name-strategy options (issue #176)")
class SubjectOptionsTest {

  private static final String VALUE_STRATEGY = "value.subject.name.strategy";
  private static final String KEY_STRATEGY = "key.subject.name.strategy";

  private final RegistryProtoFormatFactory factory = new RegistryProtoFormatFactory();

  private static Configuration urlOnly() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    return options;
  }

  private static Configuration withTopic() {
    Configuration options = urlOnly();
    options.set(ProtoConfluentFormatOptions.TOPIC, "orders");
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
        SubjectOptionsTest.class.getClassLoader(),
        false);
  }

  private static ProtoConfluentFormatConfig configOf(Object format) throws Exception {
    Field field = format.getClass().getDeclaredField("formatConfig");
    field.setAccessible(true);
    return (ProtoConfluentFormatConfig) field.get(format);
  }

  private ProtoConfluentFormatConfig sourceConfig(
      DynamicTableFactory.Context context, Configuration options) throws Exception {
    return configOf(factory.createDecodingFormat(context, options));
  }

  private ProtoConfluentFormatConfig sinkConfig(
      DynamicTableFactory.Context context, Configuration options) throws Exception {
    return configOf(factory.createEncodingFormat(context, options));
  }

  // -------------------------------------------------------------------------------------------
  // Declared options
  // -------------------------------------------------------------------------------------------

  @Test
  void onlyUrlIsRequired() {
    assertEquals(Collections.singleton(ProtoConfluentFormatOptions.URL), factory.requiredOptions());
  }

  @Test
  void topicSubjectAndStrategyAreOptionalAndForwarded() {
    for (var option :
        new Object[] {
          ProtoConfluentFormatOptions.TOPIC,
          ProtoConfluentFormatOptions.SUBJECT,
          ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY
        }) {
      assertTrue(factory.optionalOptions().contains(option), option.toString());
      assertTrue(factory.forwardOptions().contains(option), option.toString());
    }
  }

  // -------------------------------------------------------------------------------------------
  // At least one of topic / subject
  // -------------------------------------------------------------------------------------------

  @Test
  void neitherTopicNorSubject_isRejected_forSourceAndSink() {
    DynamicTableFactory.Context noTopic = tableContext(Map.of("connector", "kafka"));
    for (DynamicTableFactory.Context context : new DynamicTableFactory.Context[] {null, noTopic}) {
      ValidationException source =
          assertThrows(
              ValidationException.class, () -> factory.createDecodingFormat(context, urlOnly()));
      ValidationException sink =
          assertThrows(
              ValidationException.class, () -> factory.createEncodingFormat(context, urlOnly()));
      for (ValidationException e : new ValidationException[] {source, sink}) {
        assertTrue(e.getMessage().contains("neither 'topic' nor 'subject' is set"), e.getMessage());
        assertTrue(e.getMessage().contains("no single 'topic' to fall back to"), e.getMessage());
      }
    }
  }

  @Test
  void tableTopicFallback_isUsedWhenFormatTopicIsUnset() throws Exception {
    DynamicTableFactory.Context context =
        tableContext(Map.of("connector", "kafka", "topic", " orders "));
    assertEquals("orders", sourceConfig(context, urlOnly()).topic);
    assertEquals("orders", sinkConfig(context, urlOnly()).topic);
  }

  @Test
  void formatTopic_winsOverTableTopic() throws Exception {
    DynamicTableFactory.Context context =
        tableContext(Map.of("connector", "kafka", "topic", "table-topic"));
    assertEquals("orders", sinkConfig(context, withTopic()).topic);
  }

  @Test
  void tableTopicList_isNoFallback() {
    DynamicTableFactory.Context context =
        tableContext(Map.of("connector", "kafka", "topic", "orders;payments"));
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> factory.createDecodingFormat(context, urlOnly()));
    assertTrue(e.getMessage().contains("'orders;payments'"), e.getMessage());
    assertTrue(e.getMessage().contains("names more than one topic"), e.getMessage());
  }

  @Test
  void singleTopic_parsesTheConnectorTopicOption() {
    assertNull(RegistryProtoFormatFactory.singleTopic(null));
    assertNull(RegistryProtoFormatFactory.singleTopic(""));
    assertNull(RegistryProtoFormatFactory.singleTopic("a;b"));
    assertEquals("a", RegistryProtoFormatFactory.singleTopic("a;"));
    assertEquals("a", RegistryProtoFormatFactory.singleTopic(" a "));
  }

  @Test
  void subjectAlone_isEnough() throws Exception {
    Configuration options = urlOnly();
    options.set(ProtoConfluentFormatOptions.SUBJECT, "orders-custom");
    ProtoConfluentFormatConfig config = sourceConfig(null, options);
    assertNull(config.topic);
    assertEquals("orders-custom", config.subject);
    assertEquals("orders-custom", sinkConfig(null, options).subject);
  }

  // -------------------------------------------------------------------------------------------
  // subject routing and validation
  // -------------------------------------------------------------------------------------------

  @Test
  void subject_installsFixedStrategyForTheValue() throws Exception {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.SUBJECT, "orders-custom");
    Map<String, String> props = sinkConfig(null, options).getProperties();
    assertEquals(FixedSubjectNameStrategy.class.getName(), props.get(VALUE_STRATEGY));
    assertEquals("orders-custom", props.get(FixedSubjectNameStrategy.VALUE_SUBJECT_CONFIG));
    assertFalse(props.containsKey(KEY_STRATEGY));
    assertFalse(props.containsKey(FixedSubjectNameStrategy.KEY_SUBJECT_CONFIG));
  }

  @Test
  void subject_installsFixedStrategyForTheKey() throws Exception {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.IS_KEY, true);
    options.set(ProtoConfluentFormatOptions.SUBJECT, "orders-key-custom");
    Map<String, String> props = sinkConfig(null, options).getProperties();
    assertEquals(FixedSubjectNameStrategy.class.getName(), props.get(KEY_STRATEGY));
    assertEquals("orders-key-custom", props.get(FixedSubjectNameStrategy.KEY_SUBJECT_CONFIG));
    assertFalse(props.containsKey(VALUE_STRATEGY));
  }

  @Test
  void subject_winsOverStrategyFromProperties() throws Exception {
    Configuration options = withTopic();
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of(VALUE_STRATEGY, RecordNameStrategy.class.getName()));
    options.set(ProtoConfluentFormatOptions.SUBJECT, "orders-custom");
    assertEquals(
        FixedSubjectNameStrategy.class.getName(),
        sinkConfig(null, options).getProperties().get(VALUE_STRATEGY));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "  "})
  void blankSubject_isRejected(String subject) {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.SUBJECT, subject);
    ValidationException e =
        assertThrows(ValidationException.class, () -> factory.createEncodingFormat(null, options));
    assertTrue(e.getMessage().contains("'subject' must not be empty"), e.getMessage());
  }

  @Test
  void subjectWithStrategy_isRejected() {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.SUBJECT, "orders-custom");
    options.set(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY, "record");
    ValidationException source =
        assertThrows(ValidationException.class, () -> factory.createDecodingFormat(null, options));
    ValidationException sink =
        assertThrows(ValidationException.class, () -> factory.createEncodingFormat(null, options));
    for (ValidationException e : new ValidationException[] {source, sink}) {
      assertTrue(
          e.getMessage().contains("'subject' and 'subject-name-strategy' cannot both be set"),
          e.getMessage());
    }
  }

  // -------------------------------------------------------------------------------------------
  // subject-name-strategy routing and validation
  // -------------------------------------------------------------------------------------------

  @ParameterizedTest
  @CsvSource({
    "topic, io.confluent.kafka.serializers.subject.TopicNameStrategy",
    "TOPIC, io.confluent.kafka.serializers.subject.TopicNameStrategy",
    "record, io.confluent.kafka.serializers.subject.RecordNameStrategy",
    "Record, io.confluent.kafka.serializers.subject.RecordNameStrategy",
    "topic-record, io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
    "Topic-Record, io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
    "io.confluent.kafka.serializers.subject.RecordNameStrategy,"
        + " io.confluent.kafka.serializers.subject.RecordNameStrategy"
  })
  void strategy_isRoutedToTheValueProperty(String value, String expectedClass) throws Exception {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY, value);
    Map<String, String> props = sinkConfig(null, options).getProperties();
    assertEquals(expectedClass, props.get(VALUE_STRATEGY));
    assertFalse(props.containsKey(KEY_STRATEGY));
    assertEquals(expectedClass, sourceConfig(null, options).getProperties().get(VALUE_STRATEGY));
  }

  @Test
  void strategy_isRoutedToTheKeyPropertyWhenIsKey() throws Exception {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.IS_KEY, true);
    options.set(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY, "topic-record");
    Map<String, String> props = sinkConfig(null, options).getProperties();
    assertEquals(TopicRecordNameStrategy.class.getName(), props.get(KEY_STRATEGY));
    assertFalse(props.containsKey(VALUE_STRATEGY));
  }

  @Test
  void strategy_isAcceptedUnderItsUnderscoreKey() throws Exception {
    Configuration options = withTopic();
    options.setString("subject_name_strategy", "record");
    assertEquals(
        RecordNameStrategy.class.getName(),
        sinkConfig(null, options).getProperties().get(VALUE_STRATEGY));
  }

  @Test
  void strategy_winsOverStrategyFromProperties() throws Exception {
    Configuration options = withTopic();
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of(VALUE_STRATEGY, RecordNameStrategy.class.getName()));
    options.set(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY, "topic");
    assertEquals(
        TopicNameStrategy.class.getName(),
        sinkConfig(null, options).getProperties().get(VALUE_STRATEGY));
  }

  @Test
  void strategyFromProperties_stillWorksWithoutTypedOptions() throws Exception {
    Configuration options = withTopic();
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of(VALUE_STRATEGY, RecordNameStrategy.class.getName()));
    assertEquals(
        RecordNameStrategy.class.getName(),
        sinkConfig(null, options).getProperties().get(VALUE_STRATEGY));
  }

  @Test
  void noTypedOptions_leaveTheStrategyToConfluentsDefault() throws Exception {
    Map<String, String> props = sinkConfig(null, withTopic()).getProperties();
    assertFalse(props.containsKey(VALUE_STRATEGY));
    assertFalse(props.containsKey(KEY_STRATEGY));
  }

  @Test
  void strategy_customClassFromTheTableClassLoader_isAccepted() throws Exception {
    Configuration options = withTopic();
    options.set(
        ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY,
        FixedSubjectNameStrategy.class.getName());
    DynamicTableFactory.Context context = tableContext(Map.of("connector", "kafka"));
    assertEquals(
        FixedSubjectNameStrategy.class.getName(),
        sinkConfig(context, options).getProperties().get(VALUE_STRATEGY));
  }

  @Test
  void strategy_unknownValue_isRejected() {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY, "records");
    ValidationException e =
        assertThrows(ValidationException.class, () -> factory.createDecodingFormat(null, options));
    assertTrue(e.getMessage().contains("'subject-name-strategy' is set to 'records'"), e.getMessage());
    assertTrue(e.getMessage().contains("'topic', 'record', 'topic-record'"), e.getMessage());
  }

  @Test
  void strategy_classThatIsNoStrategy_isRejected() {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY, String.class.getName());
    ValidationException e =
        assertThrows(ValidationException.class, () -> factory.createEncodingFormat(null, options));
    assertTrue(e.getMessage().contains("does not implement"), e.getMessage());
  }

  @Test
  void strategy_blank_isRejected() {
    Configuration options = withTopic();
    options.set(ProtoConfluentFormatOptions.SUBJECT_NAME_STRATEGY, " ");
    ValidationException e =
        assertThrows(ValidationException.class, () -> factory.createEncodingFormat(null, options));
    assertTrue(e.getMessage().contains("must not be empty"), e.getMessage());
  }
}
