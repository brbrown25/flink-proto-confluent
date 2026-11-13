package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

/** Fixtures shared by the sink tests: versions of one message, rows, and wire-format decoding. */
final class SinkTestSupport {

  static final String TOPIC = "orders";
  static final String SUBJECT = TOPIC + "-value";
  static final String REGISTRY_URL = "http://stub-registry:8081";

  /** Full name every version of the hand-authored message shares. */
  static final String ORDER_FULL_NAME = "com.acme.orders.Order";

  /** Full name of the schema the sink derives from the table when the subject does not exist. */
  static final String ROW_DERIVED_FULL_NAME = "com.flink.proto.confluent.Row";

  private SinkTestSupport() {}

  /**
   * A version of the hand-authored {@code com.acme.orders.Order} message: always {@code content}
   * and {@code date_time}, plus one string field per extra name. Versions differ in their fields
   * but never in their full name.
   */
  static ProtobufSchema orderSchema(String... extraFields) {
    StringBuilder proto =
        new StringBuilder(
            "syntax = \"proto3\";\n"
                + "package com.acme.orders;\n"
                + "message Order {\n"
                + "  string content = 1;\n"
                + "  string date_time = 2;\n");
    for (int i = 0; i < extraFields.length; i++) {
      proto.append("  string ").append(extraFields[i]).append(" = ").append(i + 3).append(";\n");
    }
    return new ProtobufSchema(proto.append("}\n").toString());
  }

  /** A row type of nullable STRING columns with the given names. */
  static RowType rowType(String... columns) {
    LogicalType[] types = new LogicalType[columns.length];
    Arrays.fill(types, new VarCharType(VarCharType.MAX_LENGTH));
    return RowType.of(types, columns);
  }

  static GenericRowData row(String... values) {
    GenericRowData row = new GenericRowData(values.length);
    for (int i = 0; i < values.length; i++) {
      row.setField(i, StringData.fromString(values[i]));
    }
    return row;
  }

  static Map<String, String> serializerConfig(boolean autoRegister) {
    return Map.of(
        "schema.registry.url", REGISTRY_URL, "auto.register.schemas", String.valueOf(autoRegister));
  }

  static RowDataProtoSerializer serializer(SchemaRegistryClient client, boolean autoRegister) {
    RowDataProtoSerializer serializer = new RowDataProtoSerializer(client);
    serializer.configure(serializerConfig(autoRegister), false);
    return serializer;
  }

  static RowDataProtoSerializer serializer(
      SchemaRegistryClient client, boolean autoRegister, Duration ttl, AtomicLong nanoClock) {
    RowDataProtoSerializer serializer = new RowDataProtoSerializer(client, ttl, nanoClock::get);
    serializer.configure(serializerConfig(autoRegister), false);
    return serializer;
  }

  /** The schema ID in the Confluent wire header: magic byte, then a big-endian int. */
  static int schemaIdOf(byte[] wireBytes) {
    ByteBuffer buffer = ByteBuffer.wrap(wireBytes);
    if (buffer.get() != 0) {
      throw new IllegalArgumentException("Not Confluent wire format: bad magic byte");
    }
    return buffer.getInt();
  }

  /** Decodes wire bytes the way a consumer would, with the writer schema from the registry. */
  static DynamicMessage decode(SchemaRegistryClient client, byte[] wireBytes) {
    try (KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(client)) {
      deserializer.configure(Map.of("schema.registry.url", REGISTRY_URL), false);
      return deserializer.deserialize(TOPIC, wireBytes);
    }
  }

  static Object field(DynamicMessage message, String name) {
    return message.getField(message.getDescriptorForType().findFieldByName(name));
  }
}
