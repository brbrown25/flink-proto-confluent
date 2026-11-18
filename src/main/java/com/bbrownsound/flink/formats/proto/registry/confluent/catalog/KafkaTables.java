package com.bbrownsound.flink.formats.proto.registry.confluent.catalog;

import com.bbrownsound.flink.formats.proto.registry.confluent.RegistryProtoFormatFactory;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.ResolvedProtoSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

/**
 * Builds the Kafka connector table the catalog exposes for a topic: its columns from the resolved
 * value (and optional key) schema, and its options from the catalog config.
 *
 * <p>The Kafka connector's option names are written as strings so this artifact does not depend on
 * the connector.
 */
final class KafkaTables {

  static final String CONNECTOR = "kafka";
  private static final String FORMAT = RegistryProtoFormatFactory.IDENTIFIER;
  private static final String VALUE_PREFIX = "value." + FORMAT + ".";
  private static final String KEY_PREFIX = "key." + FORMAT + ".";

  private KafkaTables() {}

  /**
   * Builds the table for a topic.
   *
   * @param topic the Kafka topic, which is also the table name
   * @param value the schema of the {@code <topic>-value} subject
   * @param key the schema of the {@code <topic>-key} subject, or {@code null} for no key columns
   * @param config the catalog config
   * @return the table
   * @throws CatalogException if a key column would have the same name as a value column
   */
  static CatalogTable create(
      String topic,
      ResolvedProtoSchema value,
      @Nullable ResolvedProtoSchema key,
      SchemaRegistryCatalogConfig config) {
    RowType valueRow = rowType(value);
    RowType keyRow = key == null ? null : rowType(key);
    if (keyRow != null && keyRow.getFieldCount() == 0) {
      keyRow = null;
    }

    Schema.Builder schema = Schema.newBuilder();
    List<String> keyColumns = new ArrayList<>();
    if (keyRow != null) {
      Set<String> valueNames = new HashSet<>(valueRow.getFieldNames());
      for (RowType.RowField field : keyRow.getFields()) {
        String column = config.getKeyFieldsPrefix() + field.getName();
        if (valueNames.contains(column)) {
          throw new CatalogException(
              "Topic '"
                  + topic
                  + "': key field '"
                  + field.getName()
                  + "' of subject '"
                  + key.getSubject()
                  + "' would become column '"
                  + column
                  + "', which the value schema already has. Set the catalog option '"
                  + SchemaRegistryCatalogOptions.KEY_FIELDS_PREFIX.key()
                  + "' to a prefix no value field starts with, or '"
                  + SchemaRegistryCatalogOptions.KEY_ENABLED.key()
                  + "' = 'false'.");
        }
        keyColumns.add(column);
        schema.column(column, TypeConversions.fromLogicalToDataType(field.getType()));
      }
    }
    for (RowType.RowField field : valueRow.getFields()) {
      schema.column(field.getName(), TypeConversions.fromLogicalToDataType(field.getType()));
    }

    return CatalogTable.newBuilder()
        .schema(schema.build())
        .comment(comment(topic, value, keyRow == null ? null : key))
        .partitionKeys(Collections.emptyList())
        .options(
            options(topic, value.getId(), keyRow == null ? null : key.getId(), keyColumns, config))
        .build();
  }

  private static RowType rowType(ResolvedProtoSchema schema) {
    LogicalType type = ProtoToLogicalType.toLogicalType(schema.toSqlType());
    return (RowType) type;
  }

  static Map<String, String> options(
      String topic,
      int valueSchemaId,
      @Nullable Integer keySchemaId,
      List<String> keyColumns,
      SchemaRegistryCatalogConfig config) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("connector", CONNECTOR);
    options.put("topic", topic);
    if (!keyColumns.isEmpty()) {
      options.put("key.format", FORMAT);
      formatOptions(options, KEY_PREFIX, topic, keySchemaId, config);
      options.put(KEY_PREFIX + "is_key", "true");
      options.put("key.fields", String.join(";", keyColumns));
      if (!config.getKeyFieldsPrefix().isEmpty()) {
        options.put("key.fields-prefix", config.getKeyFieldsPrefix());
      }
      options.put("value.fields-include", "EXCEPT_KEY");
    }
    options.put("value.format", FORMAT);
    formatOptions(options, VALUE_PREFIX, topic, valueSchemaId, config);
    // Pass-through options last: a catalog's table.<option> may replace any generated option.
    options.putAll(config.getTableOptions());
    return options;
  }

  private static void formatOptions(
      Map<String, String> options,
      String prefix,
      String topic,
      @Nullable Integer schemaId,
      SchemaRegistryCatalogConfig config) {
    options.put(prefix + "url", config.getUrl());
    options.put(prefix + "topic", topic);
    // A sink writes with exactly the schema the columns came from, rather than looking up the
    // schema it derives from the row, which need not match the registered text byte for byte.
    options.put(prefix + "use-schema-id", String.valueOf(schemaId));
    config.getFormatConnectionOptions().forEach((name, v) -> options.put(prefix + name, v));
  }

  private static String comment(
      String topic, ResolvedProtoSchema value, @Nullable ResolvedProtoSchema key) {
    StringBuilder sb =
        new StringBuilder("Kafka topic '")
            .append(topic)
            .append("'; value: ")
            .append(describe(value));
    if (key != null) {
      sb.append("; key: ").append(describe(key));
    }
    return sb.toString();
  }

  private static String describe(ResolvedProtoSchema schema) {
    return schema.getMessageName()
        + " from subject '"
        + schema.getSubject()
        + "' version "
        + schema.getVersion()
        + " (schema ID "
        + schema.getId()
        + ")";
  }
}
