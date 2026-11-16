package com.bbrownsound.flink.formats.proto.registry.confluent.metrics;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Map;

/**
 * The registry client's HTTP layer, with every request counted and timed.
 *
 * <p>Every call the client makes to the registry (schema by ID, latest version of a subject,
 * registration, ...) goes through {@link #httpRequest}, and only calls the client could not answer
 * from its caches get here. One call is one request as the client sees it: the retries the client
 * makes on a retriable answer and the failover to the next configured registry URL happen inside
 * it, so they add to the measured time but not to the request count.
 *
 * <p>A request has failed when it ends in an exception: an I/O error, or an answer that is not a
 * success (which the client turns into a {@link RestClientException}), a 404 included.
 */
final class InstrumentedRestService extends RestService {

  private final SchemaRegistryMetrics metrics;

  InstrumentedRestService(String baseUrls, SchemaRegistryMetrics metrics) {
    super(baseUrls);
    this.metrics = metrics;
  }

  @Override
  public <T> T httpRequest(
      String path,
      String method,
      byte[] requestBodyData,
      Map<String, String> requestProperties,
      TypeReference<T> responseFormat)
      throws IOException, RestClientException {
    long start = System.nanoTime();
    boolean failed = true;
    try {
      T response =
          super.httpRequest(path, method, requestBodyData, requestProperties, responseFormat);
      failed = false;
      return response;
    } finally {
      metrics.record(System.nanoTime() - start, failed);
    }
  }
}
