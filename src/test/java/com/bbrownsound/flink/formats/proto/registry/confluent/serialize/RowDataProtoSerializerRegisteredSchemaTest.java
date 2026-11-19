package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

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

import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A sink that reads its schema from the registry writes under the ID the registry gave for it
 * (issue #210), rather than the ID of a schema rebuilt from the descriptor, whose text renders a
 * {@code map} field as a nested {@code map_entry} message and so matches no registered schema.
 */
class RowDataProtoSerializerRegisteredSchemaTest {

  private static final String MAP_SCHEMA =
      String.join(
          "\n",
          "syntax = \"proto3\";",
          "package com.acme.orders;",
          "message Order {",
          "  string content = 1;",
          "  map<string, string> attributes = 2;",
          "}");

  private static final RowType MAP_ROW_TYPE =
      RowType.of(
          new org.apache.flink.table.types.logical.LogicalType[] {
            new VarCharType(VarCharType.MAX_LENGTH),
            new MapType(
                new VarCharType(VarCharType.MAX_LENGTH), new VarCharType(VarCharType.MAX_LENGTH))
          },
          new String[] {"content", "attributes"});

  private static final RowType STRING_ROW_TYPE = rowType("content", "date_time");

  private StubSchemaRegistryClient registry;

  @BeforeEach
  void setUp() {
    registry = new StubSchemaRegistryClient();
  }

  private static RowDataProtoSerializer serializer(
      StubSchemaRegistryClient client, Map<String, String> extraConfig) {
    return serializer(client, extraConfig, null, null);
  }

  private static RowDataProtoSerializer serializer(
      StubSchemaRegistryClient client,
      Map<String, String> extraConfig,
      Duration ttl,
      AtomicLong clock) {
    RowDataProtoSerializer serializer =
        clock == null
            ? new RowDataProtoSerializer(client)
            : new RowDataProtoSerializer(client, ttl, clock::get);
    Map<String, String> config = new HashMap<>();
    config.put("schema.registry.url", REGISTRY_URL);
    config.put("normalize.schemas", "true");
    config.putAll(extraConfig);
    serializer.configure(config, false);
    return serializer;
  }

  private static GenericRowData mapRow(String content, String key, String value) {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString(content));
    row.setField(
        1, new GenericMapData(Map.of(StringData.fromString(key), StringData.fromString(value))));
    return row;
  }

  @ParameterizedTest(name = "auto.register.schemas = {0}")
  @ValueSource(booleans = {false, true})
  void mapSchema_isWrittenUnderTheRegisteredIdAndNothingIsRegistered(boolean autoRegister)
      throws Exception {
    int registeredId = registry.seed(SUBJECT, new ProtobufSchema(MAP_SCHEMA));
    RowDataProtoSerializer serializer =
        serializer(registry, Map.of("auto.register.schemas", String.valueOf(autoRegister)));

    byte[] bytes = serializer.serializeRowData(TOPIC, MAP_ROW_TYPE, mapRow("hello", "k", "v"));

    assertEquals(registeredId, schemaIdOf(bytes));
    DynamicMessage decoded = decode(registry, bytes);
    assertEquals("hello", field(decoded, "content"));
    assertEquals(1, decoded.getRepeatedFieldCount(
        decoded.getDescriptorForType().findFieldByName("attributes")));
    assertEquals(0, registry.registerCalls(), "nothing is registered");
    assertEquals(List.of(1), registry.versions(SUBJECT), "no version is added");
    // Later records reuse the cached schema and ID.
    assertEquals(
        registeredId,
        schemaIdOf(serializer.serializeRowData(TOPIC, MAP_ROW_TYPE, mapRow("again", "a", "b"))));
    assertEquals(0, registry.registerCalls());
  }

  @Test
  void useSchemaId_staysTheIdTheRecordCarries() throws Exception {
    int pinnedId = registry.seed(SUBJECT, new ProtobufSchema(MAP_SCHEMA));
    RowDataProtoSerializer serializer =
        serializer(
            registry,
            Map.of("auto.register.schemas", "false", "use.schema.id", String.valueOf(pinnedId)));

    byte[] bytes = serializer.serializeRowData(TOPIC, MAP_ROW_TYPE, mapRow("pinned", "k", "v"));

    assertEquals(pinnedId, schemaIdOf(bytes));
    assertEquals("pinned", field(decode(registry, bytes), "content"));
    assertEquals(0, registry.registerCalls());
  }

  @Test
  void useLatestWithMetadata_staysWithTheParentsChoice() throws Exception {
    Metadata metadata = new Metadata(null, Map.of("owner", "orders"), null);
    int taggedId =
        registry.seed(SUBJECT, orderSchema().copy(metadata, (RuleSet) null));
    RowDataProtoSerializer serializer =
        serializer(
            registry,
            Map.of("auto.register.schemas", "false", "use.latest.with.metadata", "owner=orders"));

    byte[] bytes = serializer.serializeRowData(TOPIC, STRING_ROW_TYPE, row("tagged", "now"));

    assertEquals(taggedId, schemaIdOf(bytes));
    assertEquals("tagged", field(decode(registry, bytes), "content"));
  }

  /**
   * While the subject is gone the ID read before is not known to be valid under it, so the parent
   * registers the schema again (auto-registration on); once the registry has it back, records are
   * written under the registry's ID without registering again.
   */
  @Test
  void subjectDeletedThenBack_registersOnceThenWritesUnderTheIdAgain() throws Exception {
    Duration ttl = Duration.ofMinutes(5);
    AtomicLong clock = new AtomicLong();
    registry.seed(SUBJECT, new ProtobufSchema(MAP_SCHEMA));
    RowDataProtoSerializer serializer =
        serializer(registry, Map.of("auto.register.schemas", "true"), ttl, clock);
    serializer.serializeRowData(TOPIC, MAP_ROW_TYPE, mapRow("first", "k", "v"));

    registry.deleteSubject(SUBJECT);
    clock.addAndGet(ttl.toNanos());
    byte[] whileGone = serializer.serializeRowData(TOPIC, MAP_ROW_TYPE, mapRow("gone", "k", "v"));
    assertEquals(1, registry.registerCalls(), "the schema is registered again");
    int restoredId = registry.getLatestSchemaMetadata(SUBJECT).getId();
    assertEquals(restoredId, schemaIdOf(whileGone));

    clock.addAndGet(ttl.toNanos());
    for (int i = 0; i < 3; i++) {
      byte[] bytes = serializer.serializeRowData(TOPIC, MAP_ROW_TYPE, mapRow("back", "k", "v"));
      assertEquals(restoredId, schemaIdOf(bytes));
    }
    assertEquals(1, registry.registerCalls(), "and not again once the subject is back");
  }
}
