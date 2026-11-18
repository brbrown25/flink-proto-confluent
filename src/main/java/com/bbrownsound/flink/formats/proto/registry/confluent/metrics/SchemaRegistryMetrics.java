package com.bbrownsound.flink.formats.proto.registry.confluent.metrics;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;

/**
 * The Schema Registry metrics of one format instance: how many requests the format sent to the
 * registry, how many of them failed, and how long they took.
 *
 * <p>The metrics are fed from the registry client's HTTP layer, below the client's caches, so a
 * lookup the client answers from its cache is not counted. See {@link InstrumentedRestService} for
 * what exactly one request is.
 */
public final class SchemaRegistryMetrics {

  /** Counter: requests sent to the Schema Registry. */
  public static final String REQUESTS = "schemaRegistryRequests";

  /** Counter: requests to the Schema Registry that failed. */
  public static final String ERRORS = "schemaRegistryErrors";

  /** Histogram: duration of the requests to the Schema Registry, in milliseconds. */
  public static final String LATENCY_MS = "schemaRegistryLatencyMs";

  private final Counter requests;
  private final Counter errors;
  private final Histogram latencyMs;

  private SchemaRegistryMetrics(MetricGroup group) {
    this.requests = group.counter(REQUESTS);
    this.errors = group.counter(ERRORS);
    this.latencyMs = group.histogram(LATENCY_MS, new LatencyHistogram());
  }

  /**
   * Creates the Schema Registry client of a source or a sink. With a metric group the client
   * reports {@value #REQUESTS}, {@value #ERRORS} and {@value #LATENCY_MS} to it; without one
   * (nothing to report to, as when a schema is opened without an initialization context) the client
   * is the plain one.
   *
   * @param schemaRegistryUrl the registry URL, or several separated by commas
   * @param properties the client properties
   * @param metricGroup the format's metric group, or {@code null} when there is none
   * @return the client
   */
  public static CachedSchemaRegistryClient createClient(
      String schemaRegistryUrl, Map<String, ?> properties, @Nullable MetricGroup metricGroup) {
    RestService restService =
        metricGroup == null
            ? new RestService(schemaRegistryUrl)
            : new InstrumentedRestService(
                schemaRegistryUrl, new SchemaRegistryMetrics(metricGroup));
    return SchemaRegistryClients.create(restService, properties);
  }

  /** Records one finished request. */
  void record(long elapsedNanos, boolean failed) {
    requests.inc();
    if (failed) {
      errors.inc();
    }
    latencyMs.update(TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
  }
}
