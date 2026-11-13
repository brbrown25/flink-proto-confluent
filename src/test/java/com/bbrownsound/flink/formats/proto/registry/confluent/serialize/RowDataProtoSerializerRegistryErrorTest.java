package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.ORDER_FULL_NAME;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.ROW_DERIVED_FULL_NAME;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.SUBJECT;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.decode;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.field;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.orderSchema;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.schemaIdOf;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.serializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * What the sink does when the Schema Registry cannot answer "what is the latest schema of this
 * subject" (issue #172). Only a "subject not found" answer (error code 40401) may lead to the
 * Row-derived schema; every other failure must fail the record with the registry's exception as the
 * cause and must never reach a registration call, whatever {@code auto.register.schemas} says.
 */
class RowDataProtoSerializerRegistryErrorTest {

  private static final RowType ROW_TYPE = rowType("content", "date_time");

  private StubSchemaRegistryClient registry;

  @BeforeEach
  void setUp() {
    registry = new StubSchemaRegistryClient();
  }

  /** Every lookup failure that is not "subject not found", for auto-register on and off. */
  static Stream<Arguments> lookupFailures() {
    return Stream.<Exception>of(
            new RestClientException("Unauthorized", 401, 40101),
            new RestClientException("Forbidden", 403, 40301),
            new RestClientException("Too Many Requests", 429, 42901),
            new RestClientException("Internal Server Error", 500, 50001),
            new RestClientException("Bad Gateway", 502, 50005),
            new RestClientException("Service Unavailable", 503, 50003),
            new RestClientException("Gateway Timeout", 504, 50005),
            // "Not found" answers that are not "subject not found": the subject is there.
            new RestClientException("Version not found", 404, 40402),
            new RestClientException("Schema not found", 404, 40403),
            // A 404 from something that is not the registry API: a proxy, an ingress, a wrong path.
            new RestClientException("HTTP 404 Not Found", 404, 404),
            new RestClientException(
                "Unable to parse error message from schema registry: '(<html>Not Found</html>)'",
                404,
                50005),
            new IOException("Unexpected end of file from server"),
            new ConnectException("Connection refused"),
            new SocketTimeoutException("Read timed out"),
            new UnknownHostException("schema-registry.internal"))
        .flatMap(failure -> Stream.of(Arguments.of(failure, true), Arguments.of(failure, false)));
  }

  @ParameterizedTest(name = "{0}, auto-register={1}")
  @MethodSource("lookupFailures")
  void serializeRowData_whenLookupFails_failsWithRegistryErrorAndNeverRegisters(
      Exception failure, boolean autoRegister) throws Exception {
    RowDataProtoSerializer serializer = serializer(registry, autoRegister);
    registry.failLookupsWith(failure);

    SerializationException thrown =
        assertThrows(
            SerializationException.class,
            () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));

    assertSame(failure, thrown.getCause(), "the registry exception must be the cause");
    assertTrue(thrown.getMessage().contains(SUBJECT), thrown.getMessage());
    assertEquals(0, registry.registerCalls(), "a failed lookup must never reach registration");
    assertTrue(registry.getAllSubjects().isEmpty(), "no subject may be created");
  }

  /**
   * The scenario from the issue: the subject already holds a hand-authored schema, the lookup fails
   * for a transient reason, and auto-registration is on. The sink used to register a Row-derived
   * schema as version 2 of that subject.
   */
  @ParameterizedTest(name = "{0}, auto-register={1}")
  @MethodSource("lookupFailures")
  void serializeRowData_whenLookupFailsOnExistingSubject_leavesHandAuthoredSchemaAlone(
      Exception failure, boolean autoRegister) throws Exception {
    int handAuthoredId = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, autoRegister);
    registry.failLookupsWith(failure);

    SerializationException thrown =
        assertThrows(
            SerializationException.class,
            () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));

    assertSame(failure, thrown.getCause());
    assertEquals(0, registry.registerCalls());
    assertEquals(List.of(1), registry.versions(SUBJECT), "no new version under the subject");
    registry.failLookupsWith(null);
    assertEquals(handAuthoredId, registry.getLatestSchemaMetadata(SUBJECT).getId());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void serializeRowData_whenLookupFails_messageNamesStatusAndErrorCode(boolean autoRegister) {
    RowDataProtoSerializer serializer = serializer(registry, autoRegister);
    registry.failLookupsWith(new RestClientException("Unauthorized", 401, 40101));

    SerializationException thrown =
        assertThrows(
            SerializationException.class,
            () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));

    assertTrue(thrown.getMessage().contains("HTTP status 401"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("error code 40101"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("Unauthorized"), thrown.getMessage());
  }

  /** A failed lookup leaves nothing behind: each following record asks the registry again. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void serializeRowData_whenLookupFails_isNotCachedAndNextRecordRetries(boolean autoRegister)
      throws Exception {
    int handAuthoredId = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, autoRegister);
    registry.failLookupsWith(new RestClientException("Service Unavailable", 503, 50003));

    assertThrows(
        SerializationException.class,
        () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));
    assertThrows(
        SerializationException.class,
        () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));
    assertEquals(2, registry.latestLookups(), "each record must retry the lookup");

    registry.failLookupsWith(null);
    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b"));

    assertEquals(3, registry.latestLookups());
    assertEquals(handAuthoredId, schemaIdOf(bytes), "the registry schema is used once it answers");
    assertEquals(ORDER_FULL_NAME, decode(registry, bytes).getDescriptorForType().getFullName());
    assertEquals(List.of(1), registry.versions(SUBJECT));
  }

  /** Not a registry answer at all, e.g. a bug in a client interceptor: never a reason to register. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void serializeRowData_whenLookupThrowsUnchecked_propagatesAndNeverRegisters(boolean autoRegister)
      throws Exception {
    RowDataProtoSerializer serializer = serializer(registry, autoRegister);
    IllegalStateException failure = new IllegalStateException("client closed");
    registry.failLookupsWith(failure);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));

    assertSame(failure, thrown);
    assertEquals(0, registry.registerCalls());
    assertTrue(registry.getAllSubjects().isEmpty());
  }

  /** A subject whose latest schema cannot be parsed as Protobuf is not "no schema". */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void serializeRowData_whenLatestSchemaIsNotParseable_failsAndNeverRegisters(boolean autoRegister)
      throws Exception {
    registry.seed(SUBJECT, orderSchema());
    registry.returnUnparseableSchemas();
    RowDataProtoSerializer serializer = serializer(registry, autoRegister);

    SerializationException thrown =
        assertThrows(
            SerializationException.class,
            () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));

    assertInstanceOf(IOException.class, thrown.getCause());
    assertTrue(thrown.getCause().getMessage().contains("Invalid schema"), thrown.getMessage());
    assertEquals(0, registry.registerCalls());
    assertEquals(List.of(1), registry.versions(SUBJECT));
  }

  /** The mock answers a missing subject the way the registry does: 404 with error code 40401. */
  @Test
  void stubRegistry_reportsMissingSubjectAsErrorCode40401() {
    RestClientException thrown =
        assertThrows(RestClientException.class, () -> registry.getLatestSchemaMetadata(SUBJECT));

    assertEquals(404, thrown.getStatus());
    assertEquals(RowDataProtoSerializer.SUBJECT_NOT_FOUND_ERROR_CODE, thrown.getErrorCode());
  }

  /** Behaviour preserved: no subject and auto-registration on registers the Row-derived schema. */
  @Test
  void serializeRowData_whenSubjectNotFoundAndAutoRegister_registersRowDerivedSchema()
      throws Exception {
    RowDataProtoSerializer serializer = serializer(registry, true);

    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("hello", "2026-01-01"));

    assertEquals(List.of(1), registry.versions(SUBJECT));
    ProtobufSchema registered =
        (ProtobufSchema) registry.getSchemaById(registry.getLatestSchemaMetadata(SUBJECT).getId());
    assertEquals(ROW_DERIVED_FULL_NAME, registered.toDescriptor().getFullName());
    assertEquals(registry.getLatestSchemaMetadata(SUBJECT).getId(), schemaIdOf(bytes));
    DynamicMessage decoded = decode(registry, bytes);
    assertEquals(ROW_DERIVED_FULL_NAME, decoded.getDescriptorForType().getFullName());
    assertEquals("hello", field(decoded, "content"));
    assertEquals("2026-01-01", field(decoded, "date_time"));
  }

  /** The same, with the error built by hand rather than by the mock. */
  @Test
  void serializeRowData_whenLookupAnswers40401AndAutoRegister_registersRowDerivedSchema()
      throws Exception {
    RowDataProtoSerializer serializer = serializer(registry, true);
    registry.failLookupsWith(new RestClientException("Subject 'orders-value' not found.", 404, 40401));

    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("hello", "2026-01-01"));

    assertEquals(List.of(1), registry.versions(SUBJECT));
    registry.failLookupsWith(null);
    assertEquals("hello", field(decode(registry, bytes), "content"));
  }

  /**
   * Behaviour preserved: no subject and auto-registration off. The Row-derived schema is looked up
   * (not registered) by the underlying serializer, the registry does not know it, and the record
   * fails with that error. Nothing is registered.
   */
  @Test
  void serializeRowData_whenSubjectNotFoundAndNoAutoRegister_failsWithoutRegistering()
      throws Exception {
    RowDataProtoSerializer serializer = serializer(registry, false);

    SerializationException thrown =
        assertThrows(
            SerializationException.class,
            () -> serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b")));

    RestClientException cause = assertInstanceOf(RestClientException.class, thrown.getCause());
    assertEquals(404, cause.getStatus());
    assertTrue(thrown.getMessage().contains("Error retrieving Protobuf schema"), thrown.toString());
    assertEquals(0, registry.registerCalls());
    assertTrue(registry.versions(SUBJECT).isEmpty(), "no schema may be registered");
  }
}
