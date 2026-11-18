package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Creates Schema Registry clients and maps the format's connection options (SSL and
 * authentication) to registry client properties.
 *
 * <p>Does not depend on Flink, so the format, the {@code ddl} command line tool and a catalog all
 * share one mapping and one way to build a client.
 */
public final class SchemaRegistryClients {

  /** Schemas the registry client keeps per cache. */
  public static final int CLIENT_CACHE_CAPACITY = 100;

  /**
   * Prefix of the format's map option whose entries are forwarded to the registry client verbatim
   * ({@code properties.<client-property>}).
   */
  public static final String PROPERTIES_PREFIX = "properties.";

  /**
   * The format's connection options, keyed by option name (without the {@code
   * value.proto-confluent.} prefix), mapped to the registry client property each one sets.
   */
  public static final Map<String, String> CONNECTION_OPTIONS;

  /** Connection options whose values are credentials. */
  public static final Set<String> SECRET_OPTIONS =
      Set.of(
          "ssl.keystore.password",
          "ssl.truststore.password",
          "basic-auth.user-info",
          "bearer-auth.token");

  static {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("ssl.keystore.location", "schema.registry.ssl.keystore.location");
    options.put("ssl.keystore.password", "schema.registry.ssl.keystore.password");
    options.put("ssl.truststore.location", "schema.registry.ssl.truststore.location");
    options.put("ssl.truststore.password", "schema.registry.ssl.truststore.password");
    options.put("basic-auth.credentials-source", "basic.auth.credentials.source");
    options.put("basic-auth.user-info", "basic.auth.user.info");
    options.put("bearer-auth.credentials-source", "bearer.auth.credentials.source");
    options.put("bearer-auth.token", "bearer.auth.token");
    CONNECTION_OPTIONS = Collections.unmodifiableMap(options);
  }

  private SchemaRegistryClients() {}

  /**
   * Returns the registry client property a connection option sets.
   *
   * @param option a key of {@link #CONNECTION_OPTIONS}, e.g. {@code basic-auth.user-info}
   * @return the client property, e.g. {@code basic.auth.user.info}
   * @throws IllegalArgumentException if the option is not a connection option
   */
  public static String clientProperty(String option) {
    String property = CONNECTION_OPTIONS.get(option);
    if (property == null) {
      throw new IllegalArgumentException("Not a Schema Registry connection option: " + option);
    }
    return property;
  }

  /**
   * Builds registry client properties from format options given by name, the way the format does:
   * {@code properties.<key>} entries are forwarded as {@code <key>}, then each connection option
   * sets its client property (and wins over a {@code properties.} entry for the same property).
   * Other options are ignored.
   *
   * @param formatOptions format options keyed by name without the {@code value.proto-confluent.}
   *     prefix, e.g. {@code basic-auth.user-info} or {@code properties.max.retries}
   * @return the client properties (empty when no option applies)
   */
  public static Map<String, String> clientProperties(Map<String, String> formatOptions) {
    Map<String, String> properties = new LinkedHashMap<>();
    formatOptions.forEach(
        (key, value) -> {
          if (key.startsWith(PROPERTIES_PREFIX)) {
            properties.put(key.substring(PROPERTIES_PREFIX.length()), value);
          }
        });
    CONNECTION_OPTIONS.forEach(
        (option, property) -> {
          String value = formatOptions.get(option);
          if (value != null) {
            properties.put(property, value);
          }
        });
    return properties;
  }

  /**
   * Creates a caching Protobuf-aware registry client.
   *
   * @param schemaRegistryUrl the registry URL, or several separated by commas
   * @param properties the client properties, e.g. from {@link #clientProperties(Map)}
   * @return the client
   */
  public static CachedSchemaRegistryClient create(
      String schemaRegistryUrl, Map<String, ?> properties) {
    return create(new RestService(schemaRegistryUrl), properties);
  }

  /**
   * Creates a caching Protobuf-aware registry client over the given REST service.
   *
   * @param restService the REST service to send requests through
   * @param properties the client properties
   * @return the client
   */
  public static CachedSchemaRegistryClient create(
      RestService restService, Map<String, ?> properties) {
    return new CachedSchemaRegistryClient(
        restService,
        CLIENT_CACHE_CAPACITY,
        Collections.singletonList(new ProtobufSchemaProvider()),
        properties,
        null);
  }
}
