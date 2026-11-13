package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.ORDER_FULL_NAME;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.REGISTRY_URL;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.SUBJECT;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.decode;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.field;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.orderSchema;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.schemaIdOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link ProtoRowDataSerializationSchema}: how the schema cache TTL reaches the serializer and the
 * Schema Registry client, and that the schema still works after the Java serialization that ships
 * it to the task managers.
 */
class ProtoRowDataSerializationSchemaTest {

  private static final Duration TTL = Duration.ofMinutes(5);
  private static final RowType ROW_TYPE = rowType("content", "date_time");

  /** What a deserialized copy of the schema talks to; reset before every test. */
  private static final Fixture FIXTURE = new Fixture();

  private static final class Fixture {
    private StubSchemaRegistryClient registry;
    private AtomicLong clock;
    private Map<String, String> clientProperties;
    private int clientsCreated;

    private void reset() {
      registry = new StubSchemaRegistryClient();
      clock = new AtomicLong(42L);
      clientProperties = null;
      clientsCreated = 0;
    }
  }

  /**
   * The schema with its two seams pointed at the fixture. It adds no state of its own, so a copy
   * that went through Java serialization behaves like the original.
   */
  private static final class StubbedSchema extends ProtoRowDataSerializationSchema {
    private static final long serialVersionUID = 1L;

    StubbedSchema(RowType rowType, ProtoConfluentFormatConfig config) {
      super(rowType, config);
    }

    @Override
    SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
      FIXTURE.clientProperties = clientProperties;
      FIXTURE.clientsCreated++;
      return FIXTURE.registry;
    }

    @Override
    LongSupplier nanoClock() {
      return FIXTURE.clock::get;
    }
  }

  @BeforeEach
  void setUp() {
    FIXTURE.reset();
  }

  private static ProtoConfluentFormatConfig config(Duration ttl, boolean autoRegister) {
    return config(REGISTRY_URL, ttl, autoRegister);
  }

  private static ProtoConfluentFormatConfig config(String url, Duration ttl, boolean autoRegister) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, url);
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, autoRegister);
    if (ttl != null) {
      options.set(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL, ttl);
    }
    return new ProtoConfluentFormatConfig(options);
  }

  private static ProtoRowDataSerializationSchema shipToTaskManager(
      ProtoRowDataSerializationSchema schema) throws Exception {
    ProtoRowDataSerializationSchema copy = InstantiationUtil.clone(schema);
    assertNotSame(schema, copy);
    copy.open(null);
    return copy;
  }

  // --- Java serialization ------------------------------------------------------------------

  @Test
  void javaSerializationRoundTrip_withTtl_cacheStillWorksAndRefreshes() throws Exception {
    int v1Id = FIXTURE.registry.seed(SUBJECT, orderSchema());
    ProtoRowDataSerializationSchema schema =
        shipToTaskManager(new StubbedSchema(ROW_TYPE, config(TTL, false)));

    for (int i = 0; i < 500; i++) {
      assertEquals(v1Id, schemaIdOf(schema.serialize(row("a", "b"))));
    }
    assertEquals(1, FIXTURE.registry.latestLookups(), "one lookup within the TTL");

    int v2Id = FIXTURE.registry.seed(SUBJECT, orderSchema("currency"));
    FIXTURE.clock.addAndGet(TTL.toNanos() - 1);
    assertEquals(v1Id, schemaIdOf(schema.serialize(row("a", "b"))));
    FIXTURE.clock.addAndGet(1);
    byte[] bytes = schema.serialize(row("hello", "2026-01-01"));

    assertEquals(v2Id, schemaIdOf(bytes));
    DynamicMessage decoded = decode(FIXTURE.registry, bytes);
    assertEquals(ORDER_FULL_NAME, decoded.getDescriptorForType().getFullName());
    assertEquals("hello", field(decoded, "content"));
    assertEquals(2, FIXTURE.registry.latestLookups());
    assertEquals(0, FIXTURE.registry.registerCalls());
  }

  @Test
  void javaSerializationRoundTrip_withoutTtl_schemaStaysPinned() throws Exception {
    int v1Id = FIXTURE.registry.seed(SUBJECT, orderSchema());
    ProtoRowDataSerializationSchema schema =
        shipToTaskManager(new StubbedSchema(ROW_TYPE, config(null, false)));
    assertEquals(v1Id, schemaIdOf(schema.serialize(row("a", "b"))));

    FIXTURE.registry.seed(SUBJECT, orderSchema("currency"));
    FIXTURE.clock.addAndGet(Duration.ofDays(365).toNanos());

    assertEquals(v1Id, schemaIdOf(schema.serialize(row("a", "b"))));
    assertEquals(1, FIXTURE.registry.latestLookups());
  }

  @Test
  void javaSerializationRoundTrip_twice_carriesTheTtl() throws Exception {
    FIXTURE.registry.seed(SUBJECT, orderSchema());
    ProtoRowDataSerializationSchema schema =
        shipToTaskManager(
            InstantiationUtil.clone(new StubbedSchema(ROW_TYPE, config(TTL, false))));
    schema.serialize(row("a", "b"));

    FIXTURE.clock.addAndGet(TTL.toNanos());
    schema.serialize(row("a", "b"));

    assertEquals(2, FIXTURE.registry.latestLookups(), "the TTL survived both round trips");
  }

  @Test
  void configBuiltFromExplicitParameters_hasNoTtlAndPinsTheSchema() throws Exception {
    int v1Id = FIXTURE.registry.seed(SUBJECT, orderSchema());
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            REGISTRY_URL,
            TOPIC,
            false,
            Map.of("schema.registry.url", REGISTRY_URL, "auto.register.schemas", "false"));
    ProtoRowDataSerializationSchema schema =
        shipToTaskManager(new StubbedSchema(ROW_TYPE, config));
    schema.serialize(row("a", "b"));

    FIXTURE.registry.seed(SUBJECT, orderSchema("currency"));
    FIXTURE.clock.addAndGet(Duration.ofDays(365).toNanos());

    assertEquals(v1Id, schemaIdOf(schema.serialize(row("a", "b"))));
    assertEquals(1, FIXTURE.registry.latestLookups());
  }

  @Test
  void open_calledTwice_keepsTheClientAndItsCache() throws Exception {
    FIXTURE.registry.seed(SUBJECT, orderSchema());
    ProtoRowDataSerializationSchema schema = new StubbedSchema(ROW_TYPE, config(TTL, false));
    schema.open(null);
    schema.serialize(row("a", "b"));

    schema.open(null);
    schema.serialize(row("a", "b"));

    assertEquals(1, FIXTURE.clientsCreated);
    assertEquals(1, FIXTURE.registry.latestLookups());
  }

  // --- registry errors through the Flink entry point -----------------------------------------

  /**
   * {@code SerializationSchema.serialize} declares no checked exception, so the registry failure
   * arrives as an unchecked Kafka {@code SerializationException} carrying the registry's exception.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void serialize_whenLookupFails_throwsUncheckedWithRegistryErrorAsCause(boolean autoRegister)
      throws Exception {
    FIXTURE.registry.seed(SUBJECT, orderSchema());
    ProtoRowDataSerializationSchema schema =
        shipToTaskManager(new StubbedSchema(ROW_TYPE, config(TTL, autoRegister)));
    RestClientException failure = new RestClientException("Internal Server Error", 500, 50001);
    FIXTURE.registry.failLookupsWith(failure);

    SerializationException thrown =
        assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));

    assertInstanceOf(RuntimeException.class, thrown);
    assertSame(failure, thrown.getCause());
    assertEquals(0, FIXTURE.registry.registerCalls());
    assertEquals(List.of(1), FIXTURE.registry.versions(SUBJECT));
  }

  /** The real client against a port nobody listens on: a connection failure, not "no schema". */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void serialize_whenRegistryIsUnreachable_failsWithTheIoError(boolean autoRegister)
      throws Exception {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }
    ProtoRowDataSerializationSchema schema =
        new ProtoRowDataSerializationSchema(
            ROW_TYPE, config("http://127.0.0.1:" + closedPort, null, autoRegister));
    schema.open(null);

    SerializationException thrown =
        assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));

    assertInstanceOf(IOException.class, thrown.getCause());
    assertTrue(thrown.getMessage().contains(SUBJECT), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("lookup of the latest schema"), thrown.getMessage());
  }

  // --- the client's own "latest version" cache -----------------------------------------------

  @Test
  void clientProperties_withoutTtl_leaveTheClientCacheAlone() {
    Map<String, String> properties =
        ProtoRowDataSerializationSchema.clientProperties(config(null, false));

    assertFalse(properties.containsKey("latest.cache.ttl.sec"));
    assertEquals(config(null, false).getProperties(), properties);
  }

  /** The client cache is capped at the TTL, between one second and the client default of 60. */
  @ParameterizedTest(name = "ttl {0} -> latest.cache.ttl.sec {1}")
  @CsvSource({
    "PT0.001S, 1",
    "PT0.999S, 1",
    "PT1S, 1",
    "PT10S, 10",
    "PT59.9S, 59",
    "PT60S, 60",
    "PT5M, 60",
    "PT24H, 60"
  })
  void clientProperties_withTtl_capTheClientLatestCache(Duration ttl, String expectedSeconds) {
    Map<String, String> properties =
        ProtoRowDataSerializationSchema.clientProperties(config(ttl, false));

    assertEquals(expectedSeconds, properties.get("latest.cache.ttl.sec"));
  }

  @Test
  void clientProperties_withTtl_keepAnExplicitClientSetting() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, REGISTRY_URL);
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    options.set(ProtoConfluentFormatOptions.SCHEMA_CACHE_TTL, Duration.ofSeconds(10));
    options.set(ProtoConfluentFormatOptions.PROPERTIES, Map.of("latest.cache.ttl.sec", "300"));

    Map<String, String> properties =
        ProtoRowDataSerializationSchema.clientProperties(new ProtoConfluentFormatConfig(options));

    assertEquals("300", properties.get("latest.cache.ttl.sec"));
  }

  @Test
  void open_handsTheCappedClientCacheToTheClientOnly() throws Exception {
    FIXTURE.registry.seed(SUBJECT, orderSchema());
    ProtoConfluentFormatConfig config = config(Duration.ofSeconds(10), false);
    ProtoRowDataSerializationSchema schema = new StubbedSchema(ROW_TYPE, config);

    schema.open(null);

    assertEquals("10", FIXTURE.clientProperties.get("latest.cache.ttl.sec"));
    assertEquals(REGISTRY_URL, FIXTURE.clientProperties.get("schema.registry.url"));
    assertFalse(config.getProperties().containsKey("latest.cache.ttl.sec"), "config is untouched");
  }

  // --- the production seams --------------------------------------------------------------------

  @Test
  void createSchemaRegistryClient_buildsACachedClient() throws Exception {
    ProtoRowDataSerializationSchema schema =
        new ProtoRowDataSerializationSchema(ROW_TYPE, config(TTL, false));

    try (SchemaRegistryClient client =
        schema.createSchemaRegistryClient(
            ProtoRowDataSerializationSchema.clientProperties(config(TTL, false)))) {
      assertInstanceOf(CachedSchemaRegistryClient.class, client);
    }
  }

  @Test
  void nanoClock_isMonotonic() {
    LongSupplier clock =
        new ProtoRowDataSerializationSchema(ROW_TYPE, config(TTL, false)).nanoClock();

    long first = clock.getAsLong();
    long second = clock.getAsLong();

    assertTrue(second - first >= 0);
  }
}
