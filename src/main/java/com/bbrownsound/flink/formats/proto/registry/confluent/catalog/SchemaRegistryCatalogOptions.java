package com.bbrownsound.flink.formats.proto.registry.confluent.catalog;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RecursionStrategy;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/** Options of the Schema Registry catalog ({@code 'type' = 'proto-confluent'}). */
public final class SchemaRegistryCatalogOptions {

  /** The Schema Registry URL, or several separated by commas. */
  public static final ConfigOption<String> URL =
      ConfigOptions.key("url")
          .stringType()
          .noDefaultValue()
          .withDescription("The URL of the Confluent Schema Registry to list subjects from.");

  /** Name of the single database the catalog exposes. */
  public static final ConfigOption<String> DEFAULT_DATABASE =
      ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
          .stringType()
          .defaultValue("default")
          .withDescription("Name of the single database that holds the catalog's tables.");

  /** Whether a protobuf {@code <topic>-key} subject adds key columns to the table. */
  public static final ConfigOption<Boolean> KEY_ENABLED =
      ConfigOptions.key("key.enabled")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether a topic whose '<topic>-key' subject holds a protobuf schema gets the key "
                  + "message's fields as columns (read and written with the Kafka connector's "
                  + "'key.format').");

  /** Prefix of the key columns. */
  public static final ConfigOption<String> KEY_FIELDS_PREFIX =
      ConfigOptions.key("key.fields-prefix")
          .stringType()
          .defaultValue("key_")
          .withDescription(
              "Prefix of the key columns, so a key field and a value field of the same name do not "
                  + "clash. Becomes the table's 'key.fields-prefix'.");

  /** How a table's columns map a field that makes a message contain itself. */
  public static final ConfigOption<String> RECURSIVE_MESSAGE =
      ConfigOptions.key(RecursionStrategy.OPTION)
          .stringType()
          .defaultValue(RecursionStrategy.FAIL.toString())
          .withDescription(
              "How a table maps a field that makes its message contain itself, which has no "
                  + "finite ROW type: 'fail' (the table fails to load), 'bytes' (BYTES, the "
                  + "serialized message), 'json' (STRING, the message's JSON) or 'row:<depth>' "
                  + "(a ROW unrolled <depth> times; deeper levels are left out).");

  /**
   * Prefixes of catalog options passed through verbatim to every table: the Kafka connector's
   * client properties and its source and sink options.
   */
  public static final List<String> KAFKA_PREFIXES = List.of("properties.", "scan.", "sink.");

  /** Prefix of catalog options passed to every table with the prefix removed. */
  public static final String TABLE_PREFIX = "table.";

  /**
   * Prefix of catalog options passed to the Schema Registry client with the prefix removed, and to
   * the format as {@code properties.<key>}.
   */
  public static final String REGISTRY_PROPERTIES_PREFIX = "schema-registry.properties.";

  /** The Schema Registry connection options (SSL and authentication), named as in the format. */
  public static final Set<ConfigOption<?>> CONNECTION_OPTIONS;

  static {
    Set<ConfigOption<?>> options = new LinkedHashSet<>();
    for (String name : SchemaRegistryClients.CONNECTION_OPTIONS.keySet()) {
      options.add(ConfigOptions.key(name).stringType().noDefaultValue());
    }
    CONNECTION_OPTIONS = Collections.unmodifiableSet(options);
  }

  private SchemaRegistryCatalogOptions() {}

  /** Returns every option prefix that is accepted without being declared as an option. */
  static String[] passThroughPrefixes() {
    List<String> prefixes = new java.util.ArrayList<>(KAFKA_PREFIXES);
    prefixes.add(TABLE_PREFIX);
    prefixes.add(REGISTRY_PROPERTIES_PREFIX);
    return prefixes.toArray(new String[0]);
  }
}
