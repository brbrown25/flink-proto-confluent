package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A sink writing to a subject with no registered schema derives one from the table and registers
 * it. That schema is proto3, so {@code NOT NULL} columns are written as plain proto3 fields, not as
 * {@code required} ones (#237).
 */
class RowDataProtoSerializerNotNullColumnsTest {

  // Magic byte, 4-byte schema id 1, message index 0.
  private static final byte[] HEADER = {0, 0, 0, 0, 1, 0};

  private MockSchemaRegistryClient registry;
  private RowDataProtoSerializer serializer;

  @BeforeEach
  void setUp() {
    registry = new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    serializer = new RowDataProtoSerializer(registry);
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081",
            "orders",
            false,
            Map.of("schema.registry.url", "http://localhost:8081"));
    serializer.configure(config.getProperties(), config.isKey);
  }

  @Test
  void zeroInNotNullColumnIsWrittenAsProto3Default() throws Exception {
    RowType rowType = new RowType(false, List.of(new RowField("id", new BigIntType(false))));
    GenericRowData row = GenericRowData.of(0L);

    byte[] bytes = serializer.serializeRowData("orders", rowType, row);

    // proto3 leaves a field at its default value off the wire; a required field would be written
    // as 08 00.
    assertArrayEquals(HEADER, bytes);
    SchemaMetadata registered = registry.getLatestSchemaMetadata("orders-value");
    assertFalse(registered.getSchema().contains("required"), registered.getSchema());
  }

  @Test
  void nullInNestedNotNullFieldDoesNotFailTheWrite() {
    // Flink enforces NOT NULL for top-level sink columns only; a NOT NULL field inside a ROW can
    // still hold null when it reaches the format. With a required field the message could not be
    // built ("Message missing required fields: x").
    RowType inner = new RowType(false, List.of(new RowField("x", new VarCharType(false, 10))));
    RowType rowType = new RowType(false, List.of(new RowField("r", inner)));
    GenericRowData row = GenericRowData.of(new GenericRowData(1));

    byte[] bytes = serializer.serializeRowData("orders", rowType, row);

    // Field 1 (r), length-delimited, holding an empty message.
    byte[] body = Arrays.copyOfRange(bytes, HEADER.length, bytes.length);
    assertArrayEquals(new byte[] {0x0a, 0x00}, body);
    assertEquals(HEADER.length + 2, bytes.length);
  }
}
