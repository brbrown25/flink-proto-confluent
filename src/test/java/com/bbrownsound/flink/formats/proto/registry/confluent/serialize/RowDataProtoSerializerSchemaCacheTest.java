package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.ORDER_FULL_NAME;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.REGISTRY_URL;
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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The sink's subject-to-schema cache and its converter cache (issue #172): how long a looked-up
 * schema is reused, when a new version is picked up, what a failed refresh does, and that
 * converters are per schema version and do not pile up. Time is a hand-moved clock; nothing sleeps.
 */
class RowDataProtoSerializerSchemaCacheTest {

  private static final Duration TTL = Duration.ofMinutes(5);
  private static final RowType ROW_TYPE = rowType("content", "date_time");

  private StubSchemaRegistryClient registry;
  private AtomicLong clock;

  @BeforeEach
  void setUp() {
    registry = new StubSchemaRegistryClient();
    // System.nanoTime has an arbitrary origin and may be negative.
    clock = new AtomicLong(-1_000_000_000L);
  }

  private void advance(Duration duration) {
    clock.addAndGet(duration.toNanos());
  }

  private byte[] write(RowDataProtoSerializer serializer) {
    return serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b"));
  }

  // --- existing subject --------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void existingSubject_usesRegistrySchemaAndAddsNoVersion(boolean autoRegister) throws Exception {
    int handAuthoredId = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, autoRegister);

    byte[] bytes = serializer.serializeRowData(TOPIC, ROW_TYPE, row("hello", "2026-01-01"));

    assertEquals(handAuthoredId, schemaIdOf(bytes));
    DynamicMessage decoded = decode(registry, bytes);
    assertEquals(ORDER_FULL_NAME, decoded.getDescriptorForType().getFullName());
    assertEquals("hello", field(decoded, "content"));
    assertEquals("2026-01-01", field(decoded, "date_time"));
    assertEquals(List.of(1), registry.versions(SUBJECT), "no version may be added");
  }

  @Test
  void existingSubject_withoutAutoRegister_neverCallsRegister() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false);

    for (int i = 0; i < 100; i++) {
      write(serializer);
    }

    assertEquals(0, registry.registerCalls());
  }

  @Test
  void existingSubject_lookedUpOnceWithinTtlAcrossManyRecords() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);

    byte[] first = write(serializer);
    for (int i = 0; i < 1_000; i++) {
      advance(Duration.ofMillis(100));
      assertArrayEquals(first, write(serializer));
    }

    assertEquals(1, registry.latestLookups(), "one lookup for 1,001 records within the TTL");
    assertEquals(1, serializer.converterCacheSize());
  }

  // --- no TTL: today's behaviour -----------------------------------------------------------

  @Test
  void withoutTtl_schemaIsPinnedAtFirstWrite() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false);
    assertEquals(v1Id, schemaIdOf(write(serializer)));

    int v2Id = registry.seed(SUBJECT, orderSchema("currency"));
    assertNotEquals(v1Id, v2Id);
    for (int i = 0; i < 100; i++) {
      assertEquals(v1Id, schemaIdOf(write(serializer)), "the first schema stays pinned");
    }

    assertEquals(1, registry.latestLookups());
  }

  @Test
  void withNullTtl_schemaIsPinnedHoweverMuchTimePasses() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, null, clock);
    assertEquals(v1Id, schemaIdOf(write(serializer)));

    registry.seed(SUBJECT, orderSchema("currency"));
    advance(Duration.ofDays(3650));

    assertEquals(v1Id, schemaIdOf(write(serializer)));
    assertEquals(1, registry.latestLookups());
  }

  /** A TTL too long to count in nanoseconds (about 292 years) means the same as no TTL. */
  @Test
  void withTtlBeyondNanosecondRange_schemaIsPinned() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer =
        serializer(registry, false, Duration.ofSeconds(Long.MAX_VALUE), clock);
    assertEquals(v1Id, schemaIdOf(write(serializer)));

    registry.seed(SUBJECT, orderSchema("currency"));
    advance(Duration.ofDays(36500));

    assertEquals(v1Id, schemaIdOf(write(serializer)));
    assertEquals(1, registry.latestLookups());
  }

  static Stream<Duration> nonPositiveTtls() {
    return Stream.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofMinutes(-5));
  }

  @ParameterizedTest
  @MethodSource("nonPositiveTtls")
  void nonPositiveTtl_isRejected(Duration ttl) {
    ValidationException thrown =
        assertThrows(
            ValidationException.class,
            () -> new RowDataProtoSerializer(registry, ttl, clock::get));

    assertTrue(thrown.getMessage().contains("positive"), thrown.getMessage());
  }

  // --- TTL expiry ----------------------------------------------------------------------------

  @Test
  void withTtl_oldSchemaIsUsedUntilTheTtlElapses_thenTheNewVersionIsPickedUp() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    assertEquals(v1Id, schemaIdOf(write(serializer)));

    int v2Id = registry.seed(SUBJECT, orderSchema("currency"));
    advance(TTL.minusNanos(1));
    byte[] beforeExpiry = write(serializer);
    assertEquals(v1Id, schemaIdOf(beforeExpiry), "one nanosecond before expiry: still version 1");
    assertNull(decode(registry, beforeExpiry).getDescriptorForType().findFieldByName("currency"));
    assertEquals(1, registry.latestLookups());

    advance(Duration.ofNanos(1));
    byte[] afterExpiry = write(serializer);
    assertEquals(v2Id, schemaIdOf(afterExpiry), "at expiry: the new schema ID is stamped");
    DynamicMessage decoded = decode(registry, afterExpiry);
    assertEquals(ORDER_FULL_NAME, decoded.getDescriptorForType().getFullName());
    assertNotNull(decoded.getDescriptorForType().findFieldByName("currency"));
    assertEquals("a", field(decoded, "content"));
    assertEquals(2, registry.latestLookups());
    assertEquals(0, registry.registerCalls());
    assertEquals(List.of(1, 2), registry.versions(SUBJECT));
  }

  /**
   * A new version that lacks a column the table writes is picked up like any other, and from then
   * on the write fails with the same descriptive error a restart would give. The sink does not
   * quietly keep version 1 or drop the column, and it registers nothing.
   */
  @Test
  void withTtl_newVersionLackingATableColumn_failsWithTheMissingColumnError() throws Exception {
    RowType withCurrency = rowType("content", "date_time", "currency");
    int v1Id = registry.seed(SUBJECT, orderSchema("currency"));
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    assertEquals(
        v1Id, schemaIdOf(serializer.serializeRowData(TOPIC, withCurrency, row("a", "b", "EUR"))));

    registry.seed(SUBJECT, orderSchema());
    advance(TTL.minusNanos(1));
    assertEquals(
        v1Id,
        schemaIdOf(serializer.serializeRowData(TOPIC, withCurrency, row("a", "b", "EUR"))),
        "before expiry the sink still writes version 1");

    advance(Duration.ofNanos(1));
    for (int i = 0; i < 3; i++) {
      IllegalArgumentException thrown =
          assertThrows(
              IllegalArgumentException.class,
              () -> serializer.serializeRowData(TOPIC, withCurrency, row("a", "b", "EUR")));
      assertTrue(thrown.getMessage().contains("'currency'"), thrown.getMessage());
      assertTrue(
          thrown.getMessage().contains("not found in target Protobuf schema"),
          thrown.getMessage());
      assertTrue(thrown.getMessage().contains(ORDER_FULL_NAME), thrown.getMessage());
    }
    assertEquals(2, registry.latestLookups(), "the new version is cached; no lookup per failure");
    assertEquals(0, registry.registerCalls());
    assertEquals(List.of(1, 2), registry.versions(SUBJECT));
  }

  /** Once a version that has the column again is registered, the next refresh recovers. */
  @Test
  void withTtl_afterAnIncompatibleVersionIsSuperseded_theSinkRecovers() throws Exception {
    RowType withCurrency = rowType("content", "date_time", "currency");
    registry.seed(SUBJECT, orderSchema("currency"));
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    serializer.serializeRowData(TOPIC, withCurrency, row("a", "b", "EUR"));

    registry.seed(SUBJECT, orderSchema());
    advance(TTL);
    assertThrows(
        IllegalArgumentException.class,
        () -> serializer.serializeRowData(TOPIC, withCurrency, row("a", "b", "EUR")));

    int v3Id = registry.seed(SUBJECT, orderSchema("currency", "note"));
    advance(TTL);
    byte[] recovered = serializer.serializeRowData(TOPIC, withCurrency, row("a", "b", "EUR"));
    assertEquals(v3Id, schemaIdOf(recovered));
    assertEquals("EUR", field(decode(registry, recovered), "currency"));
  }

  @Test
  void withTtl_lookupRepeatsOncePerTtl() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);

    for (int period = 0; period < 5; period++) {
      for (int i = 0; i < 50; i++) {
        write(serializer);
      }
      assertEquals(period + 1, registry.latestLookups());
      advance(TTL);
    }
  }

  @Test
  void withTtl_refreshThatFindsTheSameSchema_keepsDescriptorAndConverter() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    byte[] first = write(serializer);

    for (int refresh = 1; refresh <= 20; refresh++) {
      advance(TTL);
      assertArrayEquals(first, write(serializer));
      assertEquals(refresh + 1, registry.latestLookups());
      assertEquals(1, serializer.converterCacheSize());
    }
    assertEquals(v1Id, schemaIdOf(write(serializer)));
  }

  /**
   * The converter is per schema version, not per message name. The table has a column that version
   * 1 of the message lacks, so the version-1 converter cannot be built; once version 2 (same full
   * name, plus that field) is picked up, a new converter is built for it and the column is written.
   * A converter cached under the shared full name would keep failing.
   */
  @Test
  void twoVersionsWithTheSameFullName_doNotShareAConverter() throws Exception {
    RowType threeColumns = rowType("content", "date_time", "currency");
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);

    IllegalArgumentException beforeUpgrade =
        assertThrows(
            IllegalArgumentException.class,
            () -> serializer.serializeRowData(TOPIC, threeColumns, row("a", "b", "EUR")));
    assertTrue(beforeUpgrade.getMessage().contains("currency"), beforeUpgrade.getMessage());

    int v2Id = registry.seed(SUBJECT, orderSchema("currency"));
    advance(TTL);
    byte[] bytes = serializer.serializeRowData(TOPIC, threeColumns, row("a", "b", "EUR"));

    assertEquals(v2Id, schemaIdOf(bytes));
    DynamicMessage decoded = decode(registry, bytes);
    assertEquals(ORDER_FULL_NAME, decoded.getDescriptorForType().getFullName());
    assertEquals("EUR", field(decoded, "currency"));
    assertEquals(1, serializer.converterCacheSize());
  }

  @Test
  void converterBuiltForVersion1_isNotReusedForVersion2() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    assertEquals(v1Id, schemaIdOf(write(serializer)));
    assertEquals(1, serializer.converterCacheSize());

    // Version 2 moves nothing the table writes, so a stale version-1 converter would still
    // "work": it would build version-1 messages and the record would carry the version-1 ID.
    int v2Id = registry.seed(SUBJECT, orderSchema("currency"));
    advance(TTL);

    assertEquals(v2Id, schemaIdOf(write(serializer)));
    assertEquals(1, serializer.converterCacheSize(), "the version-1 converter is dropped");
  }

  @Test
  void converterCache_doesNotGrowAcrossManySchemaVersions() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);

    String[] extraFields = new String[0];
    for (int version = 2; version <= 60; version++) {
      extraFields = java.util.Arrays.copyOf(extraFields, extraFields.length + 1);
      extraFields[extraFields.length - 1] = "extra_" + version;
      int id = registry.seed(SUBJECT, orderSchema(extraFields));
      advance(TTL);

      assertEquals(id, schemaIdOf(write(serializer)));
      assertEquals(1, serializer.converterCacheSize(), "after version " + version);
    }
    assertEquals(60, registry.versions(SUBJECT).size());
  }

  // --- refresh failures: keep serving the schema already resolved ----------------------------

  static Stream<Exception> refreshFailures() {
    return Stream.of(
        new RestClientException("Unauthorized", 401, 40101),
        new RestClientException("Forbidden", 403, 40301),
        new RestClientException("Internal Server Error", 500, 50001),
        new RestClientException("Service Unavailable", 503, 50003),
        new RestClientException("Version not found", 404, 40402),
        new RestClientException("HTTP 404 Not Found", 404, 404),
        new IOException("Connection reset"),
        new SocketTimeoutException("Read timed out"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("refreshFailures")
  void withTtl_failedRefresh_keepsServingTheResolvedSchemaAndNeverRegisters(Exception failure)
      throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    byte[] first = write(serializer);

    registry.failLookupsWith(failure);
    advance(TTL);
    byte[] duringOutage = write(serializer);

    assertArrayEquals(first, duringOutage, "the record is still written with the cached schema");
    assertEquals(v1Id, schemaIdOf(duringOutage));
    assertEquals(2, registry.latestLookups(), "the refresh was attempted");
    assertEquals(0, registry.registerCalls());
    assertEquals(List.of(1), registry.versions(SUBJECT));
    assertEquals(1, serializer.converterCacheSize());
  }

  /**
   * With auto-registration on, a failed refresh still must not turn into a Row-derived version: the
   * only schema the sink hands the registry is the one it read from it.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("refreshFailures")
  void withTtl_failedRefresh_withAutoRegister_addsNoVersionToTheSubject(Exception failure)
      throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, true, TTL, clock);
    write(serializer);

    registry.failLookupsWith(failure);
    for (int i = 0; i < 10; i++) {
      advance(TTL);
      assertEquals(v1Id, schemaIdOf(write(serializer)));
    }

    registry.failLookupsWith(null);
    assertEquals(List.of(1), registry.versions(SUBJECT));
    assertEquals(v1Id, registry.getLatestSchemaMetadata(SUBJECT).getId());
    assertEquals(
        ORDER_FULL_NAME, decode(registry, write(serializer)).getDescriptorForType().getFullName());
  }

  @Test
  void withTtl_failedRefresh_isRetriedAfterTheBackoffNotOnEveryRecord() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);

    registry.failLookupsWith(new RestClientException("Service Unavailable", 503, 50003));
    advance(TTL);
    for (int i = 0; i < 200; i++) {
      assertEquals(v1Id, schemaIdOf(write(serializer)));
    }
    assertEquals(2, registry.latestLookups(), "one failed refresh, not one per record");

    advance(RowDataProtoSerializer.REFRESH_RETRY_BACKOFF.minusNanos(1));
    write(serializer);
    assertEquals(2, registry.latestLookups(), "still inside the backoff");

    advance(Duration.ofNanos(1));
    write(serializer);
    assertEquals(3, registry.latestLookups(), "retried once the backoff has passed");
  }

  @Test
  void withTtlShorterThanTheBackoff_failedRefreshIsRetriedAfterTheTtl() throws Exception {
    Duration shortTtl = Duration.ofSeconds(10);
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, shortTtl, clock);
    write(serializer);

    registry.failLookupsWith(new IOException("Connection reset"));
    advance(shortTtl);
    write(serializer);
    assertEquals(2, registry.latestLookups());

    advance(shortTtl.minusNanos(1));
    write(serializer);
    assertEquals(2, registry.latestLookups());

    advance(Duration.ofNanos(1));
    write(serializer);
    assertEquals(3, registry.latestLookups());
  }

  @Test
  void withTtl_afterTheRegistryRecovers_theNewVersionIsPickedUpAndTheTtlAppliesAgain()
      throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);
    int v2Id = registry.seed(SUBJECT, orderSchema("currency"));

    registry.failLookupsWith(new SocketTimeoutException("Read timed out"));
    advance(TTL);
    assertEquals(v1Id, schemaIdOf(write(serializer)));

    registry.failLookupsWith(null);
    advance(RowDataProtoSerializer.REFRESH_RETRY_BACKOFF);
    assertEquals(v2Id, schemaIdOf(write(serializer)));
    assertEquals(3, registry.latestLookups());

    // Back to the normal cadence: nothing until a full TTL has passed.
    advance(TTL.minusNanos(1));
    write(serializer);
    assertEquals(3, registry.latestLookups());
    advance(Duration.ofNanos(1));
    write(serializer);
    assertEquals(4, registry.latestLookups());
  }

  /** A refresh that cannot parse the new latest schema is a failed refresh like any other. */
  @Test
  void withTtl_refreshReturningAnUnparseableSchema_keepsServingTheResolvedSchema()
      throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);

    registry.seed(SUBJECT, orderSchema("currency"));
    registry.returnUnparseableSchemas();
    advance(TTL);

    assertEquals(v1Id, schemaIdOf(write(serializer)));
    assertEquals(1, serializer.converterCacheSize());
  }

  // --- the subject disappears ----------------------------------------------------------------

  /**
   * A refresh that learns the subject is gone must not send a sink that already resolved a registry
   * schema back to the Row-derived one: an operator deleting a subject in order to re-register it
   * would find the sink's guess in its place (issue #172, reached through the refresh instead of
   * the first lookup). The sink keeps the descriptor it read from the registry. With
   * auto-registration on, the Confluent serializer then puts that same schema back under the
   * subject when it resolves the ID, as any auto-registering producer would.
   */
  @Test
  void withTtl_subjectDeleted_andAutoRegister_keepsTheRegistrySchemaNeverTheRowDerivedOne()
      throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, true, TTL, clock);
    write(serializer);

    registry.deleteSubject(SUBJECT);
    for (int i = 0; i < 5; i++) {
      advance(TTL);
      assertEquals(
          ORDER_FULL_NAME,
          decode(registry, write(serializer)).getDescriptorForType().getFullName());
    }

    assertEquals(1, registry.versions(SUBJECT).size(), "one version: the schema it already had");
    assertEquals(
        orderSchema().canonicalString(), registry.getLatestSchemaMetadata(SUBJECT).getSchema());
    assertEquals(1, serializer.converterCacheSize());
  }

  /** Without auto-registration nothing can put the schema back, so the write fails loudly. */
  @Test
  void withTtl_subjectDeleted_andNoAutoRegister_failsAndRegistersNothing() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);

    registry.deleteSubject(SUBJECT);
    advance(TTL);
    SerializationException thrown =
        assertThrows(SerializationException.class, () -> write(serializer));

    assertInstanceOf(RestClientException.class, thrown.getCause());
    assertEquals(0, registry.registerCalls());
    assertTrue(registry.versions(SUBJECT).isEmpty());
  }

  @Test
  void withTtl_subjectDeletedThenReRegistered_picksUpTheNewSchema() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, true, TTL, clock);
    write(serializer);

    registry.deleteSubject(SUBJECT);
    advance(TTL);
    assertEquals(
        ORDER_FULL_NAME, decode(registry, write(serializer)).getDescriptorForType().getFullName());

    int newId = registry.seed(SUBJECT, orderSchema("currency"));
    advance(RowDataProtoSerializer.REFRESH_RETRY_BACKOFF);
    assertEquals(newId, schemaIdOf(write(serializer)));
    assertEquals(1, serializer.converterCacheSize());
  }

  /** Without a cached schema the same answer still means "missing", as at any first write. */
  @Test
  void withTtl_subjectMissingAtFirstWrite_stillFallsBackToTheRowDerivedSchema() throws Exception {
    RowDataProtoSerializer serializer = serializer(registry, true, TTL, clock);

    assertEquals(
        ROW_DERIVED_FULL_NAME,
        decode(registry, write(serializer)).getDescriptorForType().getFullName());
    assertEquals(1, registry.versions(SUBJECT).size());
  }

  // --- refresh failures that are slow or unchecked -------------------------------------------

  /**
   * The backoff counts from when the failed lookup returned. A lookup that takes longer to fail
   * than the backoff (a connect timeout) must not be due again on the very next record.
   */
  @Test
  void withTtl_refreshThatFailsSlowerThanTheBackoff_isNotRetriedOnTheNextRecord() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);

    Duration timeout = RowDataProtoSerializer.REFRESH_RETRY_BACKOFF.multipliedBy(2);
    registry.failLookupsWith(new SocketTimeoutException("Connect timed out"));
    registry.onLookup(() -> advance(timeout));
    advance(TTL);
    write(serializer);
    assertEquals(2, registry.latestLookups());

    for (int i = 0; i < 50; i++) {
      write(serializer);
    }
    assertEquals(2, registry.latestLookups(), "inside the backoff counted from the failure");

    registry.onLookup(() -> {});
    advance(RowDataProtoSerializer.REFRESH_RETRY_BACKOFF);
    write(serializer);
    assertEquals(3, registry.latestLookups());
  }

  /** An unchecked failure during a refresh keeps the working schema, like a checked one. */
  @Test
  void withTtl_refreshThatThrowsUnchecked_keepsServingTheResolvedSchema() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, true, TTL, clock);
    write(serializer);

    registry.failLookupsWith(new IllegalStateException("descriptor could not be built"));
    advance(TTL);

    assertEquals(v1Id, schemaIdOf(write(serializer)));
    assertEquals(List.of(1), registry.versions(SUBJECT));
    write(serializer);
    assertEquals(2, registry.latestLookups(), "backed off, not retried per record");
  }

  /** A subject that holds an Avro or JSON schema is not "no schema": the first write fails. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void firstLookupOfANonProtobufSubject_failsAndRegistersNothing(boolean autoRegister)
      throws Exception {
    registry.seed(SUBJECT, orderSchema());
    registry.parseSchemasAs(new AvroSchema("\"string\""));
    RowDataProtoSerializer serializer = serializer(registry, autoRegister, TTL, clock);

    SerializationException thrown =
        assertThrows(SerializationException.class, () -> write(serializer));

    assertInstanceOf(IOException.class, thrown.getCause());
    assertTrue(thrown.getCause().getMessage().contains("not PROTOBUF"), thrown.toString());
    assertEquals(0, registry.registerCalls());
    assertEquals(List.of(1), registry.versions(SUBJECT));
  }

  @Test
  void withTtl_refreshFindingANonProtobufSchema_keepsServingTheResolvedSchema() throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);

    registry.seed(SUBJECT, orderSchema("currency"));
    registry.parseSchemasAs(new AvroSchema("\"string\""));
    advance(TTL);

    assertEquals(v1Id, schemaIdOf(write(serializer)));
    assertEquals(1, serializer.converterCacheSize());
  }

  /** A Protobuf schema that yields no message descriptor (e.g. a file without messages). */
  private static ProtobufSchema schemaWithoutADescriptor() {
    return new ProtobufSchema(orderSchema().canonicalString()) {
      @Override
      public Descriptors.Descriptor toDescriptor() {
        return null;
      }
    };
  }

  @Test
  void firstLookupOfASchemaWithoutADescriptor_failsAndRegistersNothing() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    registry.parseSchemasAs(schemaWithoutADescriptor());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);

    ValidationException thrown = assertThrows(ValidationException.class, () -> write(serializer));

    assertTrue(thrown.getMessage().contains(SUBJECT), thrown.getMessage());
    assertEquals(0, registry.registerCalls());
    assertEquals(List.of(1), registry.versions(SUBJECT));
  }

  @Test
  void withTtl_refreshFindingASchemaWithoutADescriptor_keepsServingTheResolvedSchema()
      throws Exception {
    int v1Id = registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, false, TTL, clock);
    write(serializer);

    registry.seed(SUBJECT, orderSchema("currency"));
    registry.parseSchemasAs(schemaWithoutADescriptor());
    advance(TTL);

    assertEquals(v1Id, schemaIdOf(write(serializer)));
    write(serializer);
    assertEquals(2, registry.latestLookups(), "backed off, not retried per record");
  }

  /** On a first lookup there is nothing to fall back on, so an unchecked failure propagates. */
  @Test
  void firstLookupThatThrowsUnchecked_propagatesAndCachesNothing() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    RowDataProtoSerializer serializer = serializer(registry, true, TTL, clock);
    registry.failLookupsWith(new IllegalStateException("boom"));

    assertThrows(IllegalStateException.class, () -> write(serializer));
    assertEquals(List.of(1), registry.versions(SUBJECT));

    registry.failLookupsWith(null);
    write(serializer);
    assertEquals(2, registry.latestLookups(), "the failure was not cached");
  }

  /** Without a TTL the cached entry never expires, so the clock is not read per record. */
  @Test
  void withoutTtl_clockIsNotReadOnceTheSchemaIsCached() throws Exception {
    registry.seed(SUBJECT, orderSchema());
    AtomicLong reads = new AtomicLong();
    RowDataProtoSerializer serializer =
        new RowDataProtoSerializer(
            registry,
            null,
            () -> {
              reads.incrementAndGet();
              return clock.get();
            });
    serializer.configure(SinkTestSupport.serializerConfig(false), false);
    write(serializer);
    long afterFirst = reads.get();

    for (int i = 0; i < 100; i++) {
      write(serializer);
    }
    assertEquals(afterFirst, reads.get());
  }

  // --- subject created by the sink -----------------------------------------------------------

  /**
   * After the sink registers the Row-derived schema, the next record finds the subject and from
   * then on the registry's copy is cached like any other. Two converters exist at most: the one for
   * the Row-derived descriptor and the one for the descriptor read back from the registry.
   */
  @Test
  void subjectCreatedByTheSink_isLookedUpUntilItExistsThenCached() throws Exception {
    RowDataProtoSerializer serializer = serializer(registry, true, TTL, clock);

    byte[] first = write(serializer);
    assertEquals(1, registry.latestLookups(), "first record: subject not found");
    byte[] second = write(serializer);
    assertEquals(2, registry.latestLookups(), "second record: subject found and cached");
    for (int i = 0; i < 100; i++) {
      assertArrayEquals(second, write(serializer));
    }

    assertEquals(2, registry.latestLookups());
    assertArrayEquals(first, second);
    assertEquals(List.of(1), registry.versions(SUBJECT));
    assertTrue(serializer.converterCacheSize() <= 2, "got " + serializer.converterCacheSize());
  }

  // --- descriptors that do not come from the subject cache -----------------------------------

  /** No subject can be named before a schema exists, so the registry is never asked. */
  @Test
  void recordNameStrategy_reusesOneRowDerivedDescriptorAndConverter() throws Exception {
    RowDataProtoSerializer serializer = new RowDataProtoSerializer(registry, TTL, clock::get);
    serializer.configure(
        Map.of(
            "schema.registry.url",
            REGISTRY_URL,
            "auto.register.schemas",
            "true",
            "value.subject.name.strategy",
            RecordNameStrategy.class.getName()),
        false);

    byte[] first = write(serializer);
    for (int i = 0; i < 100; i++) {
      advance(TTL);
      assertArrayEquals(first, write(serializer));
    }

    assertEquals(0, registry.latestLookups());
    assertEquals(1, serializer.converterCacheSize());
    assertEquals(List.of(1), registry.versions(ROW_DERIVED_FULL_NAME));
  }

  /** A different Row type gets its own Row-derived descriptor; the old converter is dropped. */
  @Test
  void rowDerivedDescriptor_followsTheRowTypeWithoutLeakingConverters() throws Exception {
    RowDataProtoSerializer serializer = new RowDataProtoSerializer(registry);
    serializer.configure(
        Map.of(
            "schema.registry.url",
            REGISTRY_URL,
            "auto.register.schemas",
            "true",
            "value.subject.name.strategy",
            RecordNameStrategy.class.getName()),
        false);

    for (int i = 0; i < 20; i++) {
      byte[] two = serializer.serializeRowData(TOPIC, ROW_TYPE, row("a", "b"));
      assertNull(decode(registry, two).getDescriptorForType().findFieldByName("currency"));
      byte[] three =
          serializer.serializeRowData(
              TOPIC, rowType("content", "date_time", "currency"), row("a", "b", "EUR"));
      assertEquals("EUR", field(decode(registry, three), "currency"));
      assertEquals(1, serializer.converterCacheSize());
    }
  }

  /** An explicit message class is never looked up, so it has nothing to expire. */
  @Test
  void messageClass_neverAsksTheRegistryForTheLatestSchema() throws Exception {
    RowDataProtoSerializer serializer = new RowDataProtoSerializer(registry, TTL, clock::get);
    serializer.configure(
        Map.of(
            "schema.registry.url",
            REGISTRY_URL,
            "auto.register.schemas",
            "true",
            RowDataProtoSerializer.VALUE_MESSAGE_CLASS_CONFIG,
            com.bbrownsound.flink.formats.proto.test.v1.TestSimple.SimpleMessage.class.getName()),
        false);

    for (int i = 0; i < 10; i++) {
      advance(TTL);
      write(serializer);
    }

    assertEquals(0, registry.latestLookups());
    assertEquals(1, serializer.converterCacheSize());
  }
}
