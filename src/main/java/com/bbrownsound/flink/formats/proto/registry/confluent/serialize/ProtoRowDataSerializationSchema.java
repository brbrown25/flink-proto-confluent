package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.metrics.SchemaRegistryMetrics;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.time.Duration;
import java.util.Map;
import java.util.function.LongSupplier;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoRowDataSerializationSchema implements SerializationSchema<RowData> {
  private static final long serialVersionUID = 1L;

  /** Counter: rows the sink failed to serialize. */
  static final String NUM_SERIALIZE_ERRORS = "numSerializeErrors";

  /** Flink row type for the serialized data. */
  private final RowType rowType;

  /** Format and Schema Registry config. */
  private final ProtoConfluentFormatConfig formatConfig;

  private transient SchemaRegistryClient client;
  private transient RowDataProtoSerializer serializer;

  /** The format's metric group; null when the schema was opened without a context. */
  private transient MetricGroup metricGroup;

  /** Counts rows that failed to serialize; null when there is no metric group. */
  private transient Counter serializeErrors;

  /**
   * Creates a serialization schema for the given row type and config.
   *
   * @param rowType Flink row type for the data to serialize
   * @param config format and Schema Registry configuration
   */
  public ProtoRowDataSerializationSchema(RowType rowType, ProtoConfluentFormatConfig config) {
    this.rowType = rowType;
    this.formatConfig = config;
  }

  @Override
  public void open(InitializationContext context) {
    if (this.client == null) {
      if (context != null) {
        this.metricGroup = context.getMetricGroup();
        this.serializeErrors = metricGroup.counter(NUM_SERIALIZE_ERRORS);
      }
      this.client = createSchemaRegistryClient(clientProperties(formatConfig));
      this.serializer =
          new RowDataProtoSerializer(client, formatConfig.schemaCacheTtl, nanoClock());
      this.serializer.configure(formatConfig.getProperties(), formatConfig.isKey);
    }
  }

  /**
   * Serializes one row. A Schema Registry failure surfaces as an unchecked Kafka {@code
   * SerializationException} whose cause is the registry's exception, which fails the task. Every
   * row that fails, for that or any other reason, is counted in {@value #NUM_SERIALIZE_ERRORS}.
   */
  @Override
  public byte[] serialize(RowData element) {
    try {
      return this.serializer.serializeRowData(formatConfig.topic, rowType, element);
    } catch (RuntimeException e) {
      if (serializeErrors != null) {
        serializeErrors.inc();
      }
      throw e;
    }
  }

  /**
   * Properties for the Schema Registry client. The client keeps its own cache of "latest version"
   * answers ({@value SchemaRegistryClientConfig#LATEST_CACHE_TTL_CONFIG}, 60 seconds unless set),
   * which would sit in front of the sink's schema cache and delay every refresh by up to that long.
   * When a schema cache TTL is configured and the user has not set the client option themselves,
   * the client cache is capped at the TTL (never below one second) so a refresh sees the registry
   * no later than twice the TTL after a new version is registered.
   */
  static Map<String, String> clientProperties(ProtoConfluentFormatConfig config) {
    Map<String, String> properties = config.getProperties();
    Duration ttl = config.schemaCacheTtl;
    String clientTtlKey = SchemaRegistryClientConfig.LATEST_CACHE_TTL_CONFIG;
    if (ttl != null && !properties.containsKey(clientTtlKey)) {
      long clientTtlSeconds =
          Math.min(
              SchemaRegistryClientConfig.LATEST_CACHE_TTL_DEFAULT, Math.max(1L, ttl.getSeconds()));
      properties.put(clientTtlKey, Long.toString(clientTtlSeconds));
    }
    return properties;
  }

  /** Creates the Schema Registry client; a seam for tests, which substitute an in-memory one. */
  SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
    return SchemaRegistryMetrics.createClient(
        formatConfig.schemaRegistryUrl, clientProperties, metricGroup);
  }

  /** Clock that times the schema cache; a seam for tests, which move time by hand. */
  LongSupplier nanoClock() {
    return System::nanoTime;
  }
}
