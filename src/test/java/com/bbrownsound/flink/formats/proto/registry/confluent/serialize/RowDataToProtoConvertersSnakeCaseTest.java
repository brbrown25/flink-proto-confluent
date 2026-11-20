package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * The sink matches a {@code snake_case} column to a {@code camelCase} field, as the source does
 * (issue #243): top-level columns, nested {@code ROW} fields and oneof cases.
 */
class RowDataToProtoConvertersSnakeCaseTest {

  private static Descriptor schema(String... lines) {
    return (Descriptor) new ProtobufSchema(String.join("\n", lines)).toDescriptor();
  }

  private static final Descriptor ORDER =
      schema(
          "syntax = \"proto3\";",
          "package snake;",
          "message Order {",
          "  string orderId = 1;",
          "  int64 total_amount = 2;",
          "  Customer customerInfo = 3;",
          "  message Customer {",
          "    string firstName = 1;",
          "  }",
          "}");

  private static final Descriptor EVENT =
      schema(
          "syntax = \"proto3\";",
          "package snake;",
          "message Event {",
          "  string eventId = 1;",
          "  oneof payload {",
          "    string firstName = 2;",
          "    int64 itemCount = 3;",
          "  }",
          "}");

  private static final VarCharType STRING = VarCharType.STRING_TYPE;

  @Test
  void snakeCaseColumns_writeCamelCaseFields() {
    RowType customer = new RowType(List.of(new RowField("first_name", STRING)));
    RowType rowType =
        new RowType(
            List.of(
                new RowField("order_id", STRING),
                new RowField("total_amount", new BigIntType()),
                new RowField("customer_info", customer)));
    var converter = RowDataToProtoConverters.createConverter(rowType, ORDER);

    GenericRowData row =
        GenericRowData.of(
            StringData.fromString("o-1"),
            42L,
            GenericRowData.of(StringData.fromString("Ada")));
    DynamicMessage msg = (DynamicMessage) converter.convert(row);

    assertEquals("o-1", msg.getField(ORDER.findFieldByName("orderId")));
    assertEquals(42L, msg.getField(ORDER.findFieldByName("total_amount")));
    DynamicMessage inner = (DynamicMessage) msg.getField(ORDER.findFieldByName("customerInfo"));
    assertEquals(
        "Ada", inner.getField(inner.getDescriptorForType().findFieldByName("firstName")));
  }

  @Test
  void snakeCaseColumns_writeCamelCaseFieldsAndOneofCases() {
    RowType payload =
        new RowType(
            List.of(new RowField("first_name", STRING), new RowField("item_count", new BigIntType())));
    RowType rowType =
        new RowType(List.of(new RowField("event_id", STRING), new RowField("payload", payload)));
    var converter = RowDataToProtoConverters.createConverter(rowType, EVENT);

    GenericRowData row =
        GenericRowData.of(StringData.fromString("e-1"), GenericRowData.of(null, 7L));
    DynamicMessage msg = (DynamicMessage) converter.convert(row);

    assertEquals("e-1", msg.getField(EVENT.findFieldByName("eventId")));
    assertEquals(7L, msg.getField(EVENT.findFieldByName("itemCount")));
  }

  @Test
  void snakeCaseColumn_withNoCamelCaseField_stillReportedMissing() {
    RowType payload = new RowType(List.of(new RowField("last_name", STRING)));
    RowType rowType =
        new RowType(List.of(new RowField("event_ref", STRING), new RowField("payload", payload)));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> RowDataToProtoConverters.createConverter(rowType, EVENT));
    assertTrue(e.getMessage().contains("'event_ref'"), e.getMessage());
    assertTrue(e.getMessage().contains("'payload.last_name'"), e.getMessage());
  }
}
