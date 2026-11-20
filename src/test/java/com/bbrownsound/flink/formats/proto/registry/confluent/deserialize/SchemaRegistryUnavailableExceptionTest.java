package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.ConnectException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Classifying what the Confluent deserializer threw as a Schema Registry outage or a bad record
 * (GitHub issue #232).
 */
class SchemaRegistryUnavailableExceptionTest {

  private static Throwable wrapped(Throwable cause) {
    return new SerializationException("Error deserializing Protobuf message", cause);
  }

  @ParameterizedTest(name = "HTTP {0}, error code {1} -> unavailable: {2}")
  @CsvSource({
    "500, 50001, true",
    "502, 502, true",
    "503, 50003, true",
    "401, 401, true",
    "403, 40301, true",
    "407, 407, true",
    "408, 408, true",
    "429, 429, true",
    "404, 404, true",
    "404, 40401, false",
    "404, 40402, false",
    "404, 40403, false",
    "400, 40001, false",
    "409, 409, false",
    "422, 42201, false"
  })
  void restClientException_isClassifiedByStatusAndErrorCode(
      int status, int errorCode, boolean unavailable) {
    final RestClientException rest = new RestClientException("registry said no", status, errorCode);
    assertEquals(unavailable, SchemaRegistryUnavailableException.isRegistryUnavailable(rest));
    assertEquals(
        unavailable, SchemaRegistryUnavailableException.isRegistryUnavailable(wrapped(rest)));
  }

  @Test
  void restClientException_decidesBeforeAnIoExceptionFurtherDownTheChain() {
    final RestClientException rest = new RestClientException("Schema not found", 404, 40403);
    rest.initCause(new IOException("never looked at"));
    assertFalse(SchemaRegistryUnavailableException.isRegistryUnavailable(wrapped(rest)));
  }

  @Test
  void kafkaRetriableAndAuthExceptions_areUnavailable() {
    assertTrue(
        SchemaRegistryUnavailableException.isRegistryUnavailable(
            wrapped(new TimeoutException("timed out"))));
    assertTrue(
        SchemaRegistryUnavailableException.isRegistryUnavailable(
            new AuthenticationException("bad credentials")));
    assertTrue(
        SchemaRegistryUnavailableException.isRegistryUnavailable(
            wrapped(new AuthorizationException("forbidden"))));
  }

  @Test
  void invalidProtobuf_isABadRecord() {
    assertFalse(
        SchemaRegistryUnavailableException.isRegistryUnavailable(
            wrapped(new InvalidProtocolBufferException("truncated message"))));
  }

  @Test
  void otherIoException_isUnavailable() {
    assertTrue(
        SchemaRegistryUnavailableException.isRegistryUnavailable(
            wrapped(new ConnectException("Connection refused"))));
  }

  @Test
  void chainWithoutADecidingLink_isABadRecord() {
    assertFalse(
        SchemaRegistryUnavailableException.isRegistryUnavailable(
            wrapped(new IllegalStateException("Unknown magic byte!"))));
    assertFalse(SchemaRegistryUnavailableException.isRegistryUnavailable(null));
  }

  @Test
  void causeCycle_endsTheWalk() {
    final SerializationException outer = new SerializationException("outer");
    final RuntimeException inner = new RuntimeException("inner", outer);
    outer.initCause(inner);
    assertFalse(SchemaRegistryUnavailableException.isRegistryUnavailable(outer));
  }

  @Test
  void message_namesTheSchemaIdTopicAndCause() {
    final Throwable cause = new ConnectException("Connection refused");
    final SchemaRegistryUnavailableException e =
        new SchemaRegistryUnavailableException("orders", 7, cause);
    assertSame(cause, e.getCause());
    assertTrue(e.getMessage().contains("writer schema id 7"), e.getMessage());
    assertTrue(e.getMessage().contains("on topic 'orders'"), e.getMessage());
    assertTrue(e.getMessage().endsWith("Connection refused"), e.getMessage());
  }

  @Test
  void message_withoutATopic_leavesTheTopicOut() {
    final SchemaRegistryUnavailableException e =
        new SchemaRegistryUnavailableException(null, 7, new ConnectException("refused"));
    assertFalse(e.getMessage().contains("topic"), e.getMessage());
  }
}
