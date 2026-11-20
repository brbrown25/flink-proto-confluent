package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;

/**
 * The Schema Registry could not be asked for a record's writer schema: it could not be reached,
 * timed out, refused the credentials, throttled the request or failed with a 5xx. That says nothing
 * about the record, so the source fails the task whatever {@code on-deserialize-error} says, and
 * the record is neither counted in {@code numDeserializeErrors} nor sent to the dead-letter topic.
 * The restarted task reads the record again (GitHub issue #232).
 *
 * <p>A registry that answers that the schema does not exist (HTTP 404 with the registry's own
 * error code, for example 40403 "Schema not found") is not this: the record names a schema that is
 * not there, and it follows {@code on-deserialize-error} like any other bad record.
 */
public class SchemaRegistryUnavailableException extends IOException {
  private static final long serialVersionUID = 1L;

  /** The registry's "subject not found", "version not found" and "schema not found" codes. */
  private static final Set<Integer> NOT_FOUND_ERROR_CODES = Set.of(40401, 40402, 40403);

  /**
   * Creates the exception.
   *
   * @param topic the source's topic, or {@code null} when it has no single topic
   * @param schemaId the writer schema ID from the record's wire header
   * @param cause what the registry client threw
   */
  public SchemaRegistryUnavailableException(String topic, int schemaId, Throwable cause) {
    super(
        "Schema Registry request failed while resolving writer schema id "
            + schemaId
            + (topic == null ? "" : " for a record on topic '" + topic + "'")
            + "; failing the task whatever 'on-deserialize-error' says, so that the record is "
            + "read again after the restart instead of being dropped: "
            + cause.getMessage(),
        cause);
  }

  /**
   * Tells whether an exception thrown while the Confluent deserializer resolved a record's writer
   * schema means the registry could not be asked, rather than that the record is bad.
   *
   * <p>The deserializer wraps what the registry client throws. It turns an HTTP error into a
   * Kafka exception by status (401 into {@link AuthenticationException}, 403 into {@link
   * AuthorizationException}, 429, 408, 500, 502, 503 and 504 into {@link RetriableException}s, any
   * other status into a {@code SerializationException}), keeping the {@link RestClientException}
   * as the cause where it has one, and wraps an I/O error (connection refused, unknown host, TLS
   * failure, ...) in a {@code SerializationException}. The cause chain is walked and the first
   * link that decides wins:
   *
   * <ul>
   *   <li>a {@link RestClientException}: unavailable for a 5xx, 401, 403, 407, 408 or 429, and for
   *       a 404 that does not carry the registry's own not-found error code (a proxy or a wrong
   *       URL path answers every record that way); any other answer, a registry 404 such as 40403
   *       included, is about the record;
   *   <li>a Kafka {@link RetriableException}, {@link AuthenticationException} or {@link
   *       AuthorizationException}: unavailable;
   *   <li>an {@link InvalidProtocolBufferException}: the payload is not valid Protobuf, so the
   *       record is bad;
   *   <li>any other {@link IOException}: unavailable, since the only other I/O the deserializer
   *       does is talking to the registry.
   * </ul>
   *
   * Anything else (an unknown magic byte, a buffer too short for the header, a schema that does
   * not parse, ...) is about the record.
   *
   * @param error what the deserializer threw
   * @return {@code true} when the registry could not be asked
   */
  static boolean isRegistryUnavailable(Throwable error) {
    final Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable t = error; t != null && seen.add(t); t = t.getCause()) {
      if (t instanceof RestClientException) {
        final RestClientException rest = (RestClientException) t;
        return isUnavailableStatus(rest.getStatus(), rest.getErrorCode());
      }
      if (t instanceof RetriableException
          || t instanceof AuthenticationException
          || t instanceof AuthorizationException) {
        return true;
      }
      if (t instanceof InvalidProtocolBufferException) {
        return false;
      }
      if (t instanceof IOException) {
        return true;
      }
    }
    return false;
  }

  private static boolean isUnavailableStatus(int status, int errorCode) {
    if (status >= 500) {
      return true;
    }
    switch (status) {
      case 401:
      case 403:
      case 407:
      case 408:
      case 429:
        return true;
      case 404:
        return !NOT_FOUND_ERROR_CODES.contains(errorCode);
      default:
        return false;
    }
  }
}
