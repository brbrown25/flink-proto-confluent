package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.awaitility.Awaitility.await;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.ProtoRowDataSerializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class RowToProtoTest extends ProtoConfluentTestBase {

  private KafkaProtobufDeserializer<DynamicMessage> deserializer() {
    return new KafkaProtobufDeserializer<>(getSchemaRegistryClient(), getConfig());
  }

  @Test
  public void testRowToProtoWithSchemaRegistered() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var serializer =
        new ProtoRowDataSerializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor()),
            new ProtoConfluentFormatConfig(url, "simple_test", false, config));
    serializer.open(null);

    serializeMessage(TestSimple.SimpleMessage.newBuilder().build(), "simple_test");

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("field1"));
    row.setField(1, StringData.fromString("field2"));

    byte[] serialized = serializer.serialize(row);
    DynamicMessage message = deserializer().deserialize("simple_test", serialized);
    Assertions.assertEquals(
        "field1", message.getField(message.getDescriptorForType().findFieldByName("content")));
    Assertions.assertEquals(
        "field2", message.getField(message.getDescriptorForType().findFieldByName("date_time")));
  }

  private static ProtobufSchema orderSchema(String extraField) {
    return new ProtobufSchema(
        "syntax = \"proto3\";\n"
            + "package com.acme.orders;\n"
            + "message Order {\n"
            + "  string content = 1;\n"
            + "  string date_time = 2;\n"
            + (extraField == null ? "" : "  string " + extraField + " = 3;\n")
            + "}\n");
  }

  private ProtoRowDataSerializationSchema openSink(String topic, String ttl) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, getSchemaRegistryUrl());
    options.set(ProtoConfluentFormatOptions.TOPIC, topic);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, false);
    if (ttl != null) {
      options.setString("schema-cache.ttl", ttl);
    }
    var sink =
        new ProtoRowDataSerializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor()),
            new ProtoConfluentFormatConfig(options));
    sink.open(null);
    return sink;
  }

  private static GenericRowData row() {
    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("field1"));
    row.setField(1, StringData.fromString("field2"));
    return row;
  }

  /** The schema ID in the Confluent wire header: magic byte, then a big-endian int. */
  private static int schemaIdOf(byte[] wireBytes) {
    return ByteBuffer.wrap(wireBytes).getInt(1);
  }

  /**
   * Issue #172 against a real registry and the real caching client: with {@code schema-cache.ttl}
   * set, a running sink starts stamping the ID of a schema version registered after its first
   * write. This also proves the client's own "latest version" cache (60 seconds by default) does
   * not hide the new version for longer than the TTL allows.
   */
  @Test
  public void testRowToProtoPicksUpNewSchemaVersionAfterTtl() throws Exception {
    String topic = "simple_test_schema_cache_ttl";
    String subject = topic + "-value";
    var registry = getSchemaRegistryClient();
    int v1Id = registry.register(subject, orderSchema(null));
    var sink = openSink(topic, "1 s");
    Assertions.assertEquals(v1Id, schemaIdOf(sink.serialize(row())));

    int v2Id = registry.register(subject, orderSchema("currency"));
    Assertions.assertNotEquals(v1Id, v2Id);

    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .until(() -> schemaIdOf(sink.serialize(row())) == v2Id);
    DynamicMessage message = deserializer().deserialize(topic, sink.serialize(row()));
    Assertions.assertEquals("com.acme.orders.Order", message.getDescriptorForType().getFullName());
    Assertions.assertNotNull(message.getDescriptorForType().findFieldByName("currency"));
    Assertions.assertEquals(
        "field1", message.getField(message.getDescriptorForType().findFieldByName("content")));
    Assertions.assertEquals(2, registry.getAllVersions(subject).size());
  }

  /** Without {@code schema-cache.ttl} the sink keeps the schema it resolved at its first write. */
  @Test
  public void testRowToProtoKeepsFirstSchemaWithoutTtl() throws Exception {
    String topic = "simple_test_schema_cache_pinned";
    String subject = topic + "-value";
    var registry = getSchemaRegistryClient();
    int v1Id = registry.register(subject, orderSchema(null));
    var pinned = openSink(topic, null);
    var refreshing = openSink(topic, "1 s");
    Assertions.assertEquals(v1Id, schemaIdOf(pinned.serialize(row())));
    Assertions.assertEquals(v1Id, schemaIdOf(refreshing.serialize(row())));

    int v2Id = registry.register(subject, orderSchema("currency"));

    // Once the refreshing sink has seen version 2, the pinned one has had the same time to.
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofMillis(200))
        .until(() -> schemaIdOf(refreshing.serialize(row())) == v2Id);
    Assertions.assertEquals(v1Id, schemaIdOf(pinned.serialize(row())));
  }

  /**
   * Issue #172 against a real registry: a subject whose only version was soft-deleted answers the
   * latest-version lookup with "subject not found" (error code 40401), so the sink treats it like a
   * subject that never existed and falls back to the Row-derived schema. With auto-registration off
   * that schema is only looked up, the registry does not have it, and the write fails with that
   * second error rather than with a failed lookup of the latest schema.
   */
  @Test
  public void testRowToProtoSoftDeletedSubjectIsTreatedAsNotFound() throws Exception {
    String topic = "simple_test_soft_deleted";
    String subject = topic + "-value";
    var registry = getSchemaRegistryClient();
    registry.register(subject, orderSchema(null));
    registry.deleteSubject(subject);
    var sink = openSink(topic, null);

    SerializationException thrown =
        Assertions.assertThrows(SerializationException.class, () -> sink.serialize(row()));
    Assertions.assertTrue(
        thrown.getMessage().startsWith("Error retrieving Protobuf schema"), thrown.getMessage());
    Assertions.assertTrue(
        thrown.getMessage().contains("package com.flink.proto.confluent"), thrown.getMessage());
    Assertions.assertInstanceOf(RestClientException.class, thrown.getCause());
    Assertions.assertFalse(getSchemaRegistryClient().getAllSubjects().contains(subject));
  }

  @Test
  public void testRowToProtoNoSchemaRegistered() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var serializer =
        new ProtoRowDataSerializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor()),
            new ProtoConfluentFormatConfig(url, "simple_test_not_registered", false, config));
    serializer.open(null);

    serializeMessage(TestSimple.SimpleMessage.newBuilder().build(), "simple_test_not_registered");

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("field1"));
    row.setField(1, StringData.fromString("field2"));

    byte[] serialized = serializer.serialize(row);
    DynamicMessage message = deserializer().deserialize("simple_test_not_registered", serialized);
    Assertions.assertEquals(
        "field1", message.getField(message.getDescriptorForType().findFieldByName("content")));
    Assertions.assertEquals(
        "field2", message.getField(message.getDescriptorForType().findFieldByName("date_time")));
  }
}
