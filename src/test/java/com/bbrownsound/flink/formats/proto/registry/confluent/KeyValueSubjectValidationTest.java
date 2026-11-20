package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
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
 * Covers GitHub issue #241: a {@code key.format = 'proto-confluent'} without {@code is_key = true}
 * resolves the value's subject, and a table whose key and value formats resolve one subject would
 * register (or check) the key schema as a version of the value's subject. Both are rejected when
 * the table is planned, by the key format and by the value format alike.
 *
 * <p>The format options are built the way Flink's {@code TableFactoryHelper} builds them: a view
 * of the table's options under the format's prefix ({@code key.proto-confluent.}, {@code
 * value.proto-confluent.} or, for the connector's {@code format} option, {@code proto-confluent.}).
 */
@DisplayName("key and value formats must not share a subject (issue #241)")
class KeyValueSubjectValidationTest {

  private static final String KEY = "key.proto-confluent.";
  private static final String VALUE = "value.proto-confluent.";

  private final RegistryProtoFormatFactory factory = new RegistryProtoFormatFactory();

  /** A Kafka table on {@code orders} with proto-confluent key and value formats. */
  private static Map<String, String> keyedTable() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", "kafka");
    options.put("topic", "orders");
    options.put("key.format", "proto-confluent");
    options.put("key.fields", "id");
    options.put(KEY + "url", "http://localhost:8081");
    options.put(KEY + "is_key", "true");
    options.put("value.format", "proto-confluent");
    options.put(VALUE + "url", "http://localhost:8081");
    return options;
  }

  private static DynamicTableFactory.Context tableContext(Map<String, String> tableOptions) {
    ResolvedCatalogTable table =
        new ResolvedCatalogTable(
            CatalogTable.newBuilder()
                .schema(Schema.newBuilder().column("id", DataTypes.STRING()).build())
                .options(tableOptions)
                .build(),
            ResolvedSchema.of(Column.physical("id", DataTypes.STRING())));
    return new FactoryUtil.DefaultDynamicTableContext(
        ObjectIdentifier.of("cat", "db", "t"),
        table,
        Collections.emptyMap(),
        new Configuration(),
        KeyValueSubjectValidationTest.class.getClassLoader(),
        false);
  }

  private static DelegatingConfiguration formatOptions(Map<String, String> table, String prefix) {
    return new DelegatingConfiguration(Configuration.fromMap(table), prefix);
  }

  /** Creates the source and the sink format under {@code prefix}, expecting both to fail. */
  private ValidationException[] rejectedBoth(Map<String, String> table, String prefix) {
    DynamicTableFactory.Context context = tableContext(table);
    return new ValidationException[] {
      assertThrows(
          ValidationException.class,
          () -> factory.createDecodingFormat(context, formatOptions(table, prefix))),
      assertThrows(
          ValidationException.class,
          () -> factory.createEncodingFormat(context, formatOptions(table, prefix)))
    };
  }

  private void acceptedBoth(Map<String, String> table, String prefix) {
    DynamicTableFactory.Context context = tableContext(table);
    assertDoesNotThrow(() -> factory.createDecodingFormat(context, formatOptions(table, prefix)));
    assertDoesNotThrow(() -> factory.createEncodingFormat(context, formatOptions(table, prefix)));
  }

  private static void assertMessageContains(ValidationException[] errors, String... parts) {
    for (ValidationException e : errors) {
      for (String part : parts) {
        assertTrue(e.getMessage().contains(part), e.getMessage());
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // key.format without is_key = true
  // -------------------------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {KEY, VALUE})
  void keyFormatWithoutIsKey_isRejected(String prefix) {
    Map<String, String> table = keyedTable();
    table.remove(KEY + "is_key");
    assertMessageContains(
        rejectedBoth(table, prefix),
        "'key.format' is 'proto-confluent' but 'key.proto-confluent.is_key' is not set",
        "'orders-value'",
        "Add 'key.proto-confluent.is_key' = 'true'");
  }

  @ParameterizedTest
  @ValueSource(strings = {"false", "yes", " "})
  void keyFormatWithIsKeyNotTrue_isRejected(String isKey) {
    Map<String, String> table = keyedTable();
    table.put(KEY + "is_key", isKey);
    assertMessageContains(
        rejectedBoth(table, VALUE),
        "'key.proto-confluent.is_key' is set to '" + isKey + "'",
        "Add 'key.proto-confluent.is_key' = 'true'");
  }

  @Test
  void keyFormatWithoutIsKeyOrTopic_namesTheValueSubjectGenerically() {
    Map<String, String> table = keyedTable();
    table.remove(KEY + "is_key");
    table.remove("topic");
    table.put(VALUE + "subject", "orders-value");
    table.put(KEY + "subject", "orders-key");
    assertMessageContains(
        rejectedBoth(table, VALUE),
        "would resolve its subject as a value subject",
        "Add 'key.proto-confluent.is_key' = 'true'");
  }

  @ParameterizedTest
  @ValueSource(strings = {"true", "TRUE"})
  void keyFormatWithIsKeyTrue_isAccepted(String isKey) {
    Map<String, String> table = keyedTable();
    table.put(KEY + "is_key", isKey);
    acceptedBoth(table, KEY);
    acceptedBoth(table, VALUE);
  }

  @Test
  void otherKeyFormat_isNotChecked() {
    Map<String, String> table = keyedTable();
    table.put("key.format", "raw");
    table.remove(KEY + "is_key");
    acceptedBoth(table, VALUE);
  }

  @Test
  void tableWithoutKeyFormat_isNotChecked() {
    Map<String, String> table = keyedTable();
    table.remove("key.format");
    acceptedBoth(table, VALUE);
  }

  @Test
  void withoutContext_nothingIsChecked() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "orders");
    assertDoesNotThrow(() -> factory.createDecodingFormat(null, options));
    assertDoesNotThrow(() -> factory.createEncodingFormat(null, options));
  }

  // -------------------------------------------------------------------------------------------
  // key and value resolving the same subject
  // -------------------------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {KEY, VALUE})
  void valueFormatWithIsKeyTrue_resolvesTheKeySubject_andIsRejected(String prefix) {
    Map<String, String> table = keyedTable();
    table.put(VALUE + "is_key", "true");
    assertMessageContains(
        rejectedBoth(table, prefix),
        "both resolve the Schema Registry subject 'orders-key'",
        "'value.proto-confluent.is_key'");
  }

  @Test
  void sameExplicitSubject_isRejected() {
    Map<String, String> table = keyedTable();
    table.put(KEY + "subject", "orders");
    table.put(VALUE + "subject", "orders");
    assertMessageContains(
        rejectedBoth(table, KEY), "both resolve the Schema Registry subject 'orders'");
  }

  @Test
  void keySubjectNamingTheValueTopicSubject_isRejected() {
    Map<String, String> table = keyedTable();
    table.put(KEY + "subject", "orders-value");
    assertMessageContains(
        rejectedBoth(table, VALUE), "both resolve the Schema Registry subject 'orders-value'");
  }

  @Test
  void formatTopicsAreUsedOverTheTableTopic() {
    Map<String, String> table = keyedTable();
    table.remove("topic");
    table.put(KEY + "topic", "orders");
    table.put(VALUE + "topic", "orders");
    table.put(VALUE + "is_key", "true");
    assertMessageContains(
        rejectedBoth(table, VALUE), "both resolve the Schema Registry subject 'orders-key'");
  }

  @Test
  void connectorFormatOption_isTheValueFormat() {
    Map<String, String> table = keyedTable();
    table.remove("value.format");
    table.remove(VALUE + "url");
    table.put("format", "proto-confluent");
    table.put("proto-confluent.url", "http://localhost:8081");
    table.put("proto-confluent.subject", "orders-key");
    assertMessageContains(
        rejectedBoth(table, "proto-confluent."),
        "both resolve the Schema Registry subject 'orders-key'");
  }

  @Test
  void distinctSubjects_areAccepted() {
    Map<String, String> table = keyedTable();
    table.put(KEY + "subject", "orders-k");
    table.put(VALUE + "subject", "orders-v");
    acceptedBoth(table, KEY);
    acceptedBoth(table, VALUE);
  }

  @Test
  void topicStrategyByAlias_stillDerivesTheSubject() {
    Map<String, String> table = keyedTable();
    table.put(VALUE + "subject-name-strategy", "TOPIC");
    table.put(VALUE + "is_key", "true");
    assertMessageContains(
        rejectedBoth(table, KEY), "both resolve the Schema Registry subject 'orders-key'");
  }

  @Test
  void topicStrategyThroughProperties_stillDerivesTheSubject() {
    Map<String, String> table = keyedTable();
    table.put(
        VALUE + "properties.key.subject.name.strategy",
        "io.confluent.kafka.serializers.subject.TopicNameStrategy");
    table.put(VALUE + "is_key", "true");
    assertMessageContains(
        rejectedBoth(table, KEY), "both resolve the Schema Registry subject 'orders-key'");
  }

  @Test
  void recordStrategy_isNotKnownWhenPlanned_andIsAccepted() {
    Map<String, String> table = keyedTable();
    table.put(KEY + "subject-name-strategy", "record");
    table.put(VALUE + "subject-name-strategy", "record");
    acceptedBoth(table, KEY);
    acceptedBoth(table, VALUE);
  }

  @Test
  void customStrategyThroughProperties_isAccepted() {
    Map<String, String> table = keyedTable();
    table.put(
        VALUE + "properties.value.subject.name.strategy",
        "io.confluent.kafka.serializers.subject.RecordNameStrategy");
    acceptedBoth(table, VALUE);
  }

  @Test
  void unrelatedProperties_keepTheTopicStrategy() {
    Map<String, String> table = keyedTable();
    table.put(VALUE + "properties.basic.auth.credentials.source", "USER_INFO");
    table.put(VALUE + "is_key", "true");
    assertMessageContains(
        rejectedBoth(table, KEY), "both resolve the Schema Registry subject 'orders-key'");
  }

  @Test
  void otherValueFormat_isNotCompared() {
    Map<String, String> table = keyedTable();
    table.put("value.format", "json");
    table.put(KEY + "subject", "orders-value");
    acceptedBoth(table, KEY);
  }

  @Test
  void valueFormatWithoutTopic_isNotCompared() {
    Map<String, String> table = keyedTable();
    table.put("topic", "orders;payments");
    table.put(KEY + "subject", "orders-value");
    acceptedBoth(table, KEY);
  }
}
