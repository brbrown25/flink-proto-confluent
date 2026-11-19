package com.bbrownsound.flink.formats.proto.registry.confluent.catalog;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RecursionStrategy;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The settings of a Schema Registry catalog, split out of its {@code CREATE CATALOG} options.
 *
 * <p>Holds Schema Registry credentials, so it deliberately has no {@code toString()} that prints
 * them.
 */
public final class SchemaRegistryCatalogConfig {

  private final String url;
  private final String defaultDatabase;
  private final boolean keyEnabled;
  private final String keyFieldsPrefix;
  private final RecursionStrategy recursion;
  private final Map<String, String> formatConnectionOptions;
  private final Map<String, String> tableOptions;

  private SchemaRegistryCatalogConfig(
      String url,
      String defaultDatabase,
      boolean keyEnabled,
      String keyFieldsPrefix,
      RecursionStrategy recursion,
      Map<String, String> formatConnectionOptions,
      Map<String, String> tableOptions) {
    this.url = url;
    this.defaultDatabase = defaultDatabase;
    this.keyEnabled = keyEnabled;
    this.keyFieldsPrefix = keyFieldsPrefix;
    this.recursion = recursion;
    this.formatConnectionOptions = Collections.unmodifiableMap(formatConnectionOptions);
    this.tableOptions = Collections.unmodifiableMap(tableOptions);
  }

  /**
   * Splits catalog options into catalog settings, Schema Registry connection options and the
   * options passed through to every table. Unknown options are not rejected here; the factory
   * validates them first.
   *
   * @param options the {@code CREATE CATALOG} options, without {@code type}
   * @return the config
   * @throws IllegalArgumentException if {@code url} is missing or {@code recursive-message} is not
   *     a valid value
   */
  public static SchemaRegistryCatalogConfig fromOptions(Map<String, String> options) {
    String url = options.get(SchemaRegistryCatalogOptions.URL.key());
    if (url == null || url.isBlank()) {
      throw new IllegalArgumentException(
          "The Schema Registry catalog needs the '"
              + SchemaRegistryCatalogOptions.URL.key()
              + "' option.");
    }
    String database =
        options.getOrDefault(
            SchemaRegistryCatalogOptions.DEFAULT_DATABASE.key(),
            SchemaRegistryCatalogOptions.DEFAULT_DATABASE.defaultValue());
    boolean keyEnabled =
        Boolean.parseBoolean(
            options.getOrDefault(
                SchemaRegistryCatalogOptions.KEY_ENABLED.key(),
                String.valueOf(SchemaRegistryCatalogOptions.KEY_ENABLED.defaultValue())));
    String keyPrefix =
        options.getOrDefault(
            SchemaRegistryCatalogOptions.KEY_FIELDS_PREFIX.key(),
            SchemaRegistryCatalogOptions.KEY_FIELDS_PREFIX.defaultValue());
    RecursionStrategy recursion =
        RecursionStrategy.parse(
            options.getOrDefault(
                SchemaRegistryCatalogOptions.RECURSIVE_MESSAGE.key(),
                SchemaRegistryCatalogOptions.RECURSIVE_MESSAGE.defaultValue()));

    Map<String, String> connection = new LinkedHashMap<>();
    Map<String, String> table = new LinkedHashMap<>();
    Map<String, String> tableOverrides = new LinkedHashMap<>();
    options.forEach(
        (key, value) -> {
          if (SchemaRegistryClients.CONNECTION_OPTIONS.containsKey(key)) {
            connection.put(key, value);
          } else if (key.startsWith(SchemaRegistryCatalogOptions.REGISTRY_PROPERTIES_PREFIX)) {
            connection.put(
                SchemaRegistryClients.PROPERTIES_PREFIX
                    + key.substring(SchemaRegistryCatalogOptions.REGISTRY_PROPERTIES_PREFIX.length()),
                value);
          } else if (key.startsWith(SchemaRegistryCatalogOptions.TABLE_PREFIX)) {
            tableOverrides.put(
                key.substring(SchemaRegistryCatalogOptions.TABLE_PREFIX.length()), value);
          } else if (SchemaRegistryCatalogOptions.KAFKA_PREFIXES.stream()
              .anyMatch(key::startsWith)) {
            table.put(key, value);
          }
        });
    // A table.<option> wins over a pass-through option of the same name.
    table.putAll(tableOverrides);
    return new SchemaRegistryCatalogConfig(
        url, database, keyEnabled, keyPrefix, recursion, connection, table);
  }

  /** Returns the Schema Registry URL. */
  public String getUrl() {
    return url;
  }

  /** Returns the name of the catalog's only database. */
  public String getDefaultDatabase() {
    return defaultDatabase;
  }

  /** Returns whether a protobuf {@code <topic>-key} subject adds key columns. */
  public boolean isKeyEnabled() {
    return keyEnabled;
  }

  /** Returns the prefix of key columns (may be empty). */
  public String getKeyFieldsPrefix() {
    return keyFieldsPrefix;
  }

  /** Returns how table columns map a field that makes a message contain itself. */
  public RecursionStrategy getRecursionStrategy() {
    return recursion;
  }

  /**
   * Returns the Schema Registry connection options named as the format names them (without the
   * {@code value.proto-confluent.} prefix), e.g. {@code basic-auth.user-info} or {@code
   * properties.max.retries}.
   */
  public Map<String, String> getFormatConnectionOptions() {
    return formatConnectionOptions;
  }

  /** Returns the Schema Registry client properties derived from the connection options. */
  public Map<String, String> getClientProperties() {
    return SchemaRegistryClients.clientProperties(formatConnectionOptions);
  }

  /** Returns the options passed through to every table, in the order they were given. */
  public Map<String, String> getTableOptions() {
    return tableOptions;
  }
}
