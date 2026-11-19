package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * One writer schema that defines two messages, with records of both under the same schema ID: the
 * record's message indexes pick the message, so each record must be converted with its own
 * message's descriptor and not with the one of the first record seen for that ID (GitHub issue
 * #211).
 */
class ProtoRowDataDeserializationSchemaMultiMessageTest {

  /**
   * The two messages share column names but not field numbers or types, so a record converted with
   * the other message's descriptor cannot come out right.
   */
  private static final String SCHEMA =
      "syntax = \"proto3\";\n"
          + "package bug211.v1;\n"
          + "message Order {\n"
          + "  string id = 1;\n"
          + "  int64 amount = 2;\n"
          + "}\n"
          + "message Refund {\n"
          + "  int64 amount = 1;\n"
          + "  string id = 2;\n"
          + "  string reason = 3;\n"
          + "}\n";

  private static final RowType ROW =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("id", DataTypes.STRING()),
                  DataTypes.FIELD("amount", DataTypes.BIGINT()))
              .getLogicalType();

  private MockSchemaRegistryClient registry;
  private int schemaId;
  private byte[] order;
  private byte[] refund;

  @BeforeEach
  void setUp() throws Exception {
    registry =
        new MockSchemaRegistryClient(Collections.singletonList(new ProtobufSchemaProvider()));
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    schemaId = registry.register("payments-value", schema);

    Descriptor orderType = schema.toDescriptor("bug211.v1.Order");
    Descriptor refundType = schema.toDescriptor("bug211.v1.Refund");
    order =
        record(
            0,
            DynamicMessage.newBuilder(orderType)
                .setField(orderType.findFieldByName("id"), "order-1")
                .setField(orderType.findFieldByName("amount"), 100L)
                .build());
    refund =
        record(
            1,
            DynamicMessage.newBuilder(refundType)
                .setField(refundType.findFieldByName("id"), "refund-1")
                .setField(refundType.findFieldByName("amount"), 40L)
                .setField(refundType.findFieldByName("reason"), "damaged")
                .build());
  }

  /** Confluent wire format: magic byte, schema ID, message indexes, then the message. */
  private byte[] record(int messageIndex, DynamicMessage message) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(0);
    out.writeInt(schemaId);
    out.write(new MessageIndexes(List.of(messageIndex)).toByteArray());
    message.writeTo(out);
    out.flush();
    return bytes.toByteArray();
  }

  private ProtoConfluentFormatConfig config() {
    return new ProtoConfluentFormatConfig(
        "http://localhost:8081",
        "payments",
        false,
        Map.of("schema.registry.url", "http://localhost:8081"));
  }

  private ProtoRowDataDeserializationSchema schema(
      int[][] projections, ProtoConfluentFormatConfig config) {
    return new ProtoRowDataDeserializationSchema(ROW, projections, null, config) {
      @Override
      SchemaRegistryClient createSchemaRegistryClient(InitializationContext context) {
        return registry;
      }
    };
  }

  private static DeserializationSchema.InitializationContext contextFor(MetricListener listener) {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return listener.getMetricGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(
            ProtoRowDataDeserializationSchemaMultiMessageTest.class.getClassLoader());
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static int cachedSchemas(MetricListener listener) {
    Gauge<Integer> gauge =
        (Gauge<Integer>)
            (Gauge<?>)
                listener
                    .getGauge(ProtoRowDataDeserializationSchema.NUM_CACHED_SCHEMAS)
                    .orElseThrow();
    return gauge.getValue();
  }

  private static void assertRow(RowData row, String id, long amount) {
    assertEquals(id, row.getString(0).toString());
    assertEquals(amount, row.getLong(1));
  }

  @Test
  void bothRecordsCarryTheSameSchemaId() {
    assertEquals(order[1], refund[1]);
    assertEquals(order[4], refund[4]);
  }

  @Test
  void eachRecordConvertsWithItsOwnMessage_whicheverComesFirst() throws IOException {
    MetricListener listener = new MetricListener();
    ProtoRowDataDeserializationSchema orderFirst = schema(null, config());
    orderFirst.open(contextFor(listener));
    assertEquals(0, cachedSchemas(listener));

    assertRow(orderFirst.deserialize(order), "order-1", 100L);
    assertEquals(1, cachedSchemas(listener));
    assertRow(orderFirst.deserialize(refund), "refund-1", 40L);
    assertEquals(2, cachedSchemas(listener), "One converter per schema ID and message");
    assertRow(orderFirst.deserialize(order), "order-1", 100L);
    assertRow(orderFirst.deserialize(refund), "refund-1", 40L);
    assertEquals(2, cachedSchemas(listener), "Converters are reused");

    ProtoRowDataDeserializationSchema refundFirst = schema(null, config());
    refundFirst.open(null);
    assertRow(refundFirst.deserialize(refund), "refund-1", 40L);
    assertRow(refundFirst.deserialize(order), "order-1", 100L);
  }

  @Test
  void projectedRead_convertsEachRecordWithItsOwnMessage() throws IOException {
    // The planner pushed down a projection to the 'amount' column only.
    ProtoRowDataDeserializationSchema projected = schema(new int[][] {{1}}, config());
    projected.open(null);

    RowData fromOrder = projected.deserialize(order);
    RowData fromRefund = projected.deserialize(refund);
    assertEquals(1, fromOrder.getArity());
    assertEquals(100L, fromOrder.getLong(0));
    assertEquals(40L, fromRefund.getLong(0));
  }

  @Test
  void messageName_stillRejectsTheOtherMessageUnderTheSameSchemaId() throws IOException {
    ProtoConfluentFormatConfig config = config();
    config.messageName = "bug211.v1.Order";
    ProtoRowDataDeserializationSchema schema = schema(null, config);
    schema.open(null);

    assertRow(schema.deserialize(order), "order-1", 100L);
    IOException e = assertThrows(IOException.class, () -> schema.deserialize(refund));
    assertInstanceOf(UnexpectedMessageTypeException.class, e.getCause());

    config.onDeserializeError = OnDeserializeError.SKIP;
    assertNull(schema.deserialize(refund));
    assertRow(schema.deserialize(order), "order-1", 100L);
  }
}
