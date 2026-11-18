package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoToRowDataConverters;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.SourceMessageValidator;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.RowDataToProtoConverters;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RowTypeToProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

/**
 * Column types a table can declare for a Protobuf field in place of the derived one (issue #174):
 * each is read from and written back to the same Protobuf value, accepted by the {@code
 * message-class} check, and (where it has one) derived to the Protobuf type it came from.
 */
class TypeCoverageTest {

  private static final String CLASS_NAME = "com.example.Coverage";

  private static final Descriptor COVERAGE =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package coverage;",
                      "import \"google/protobuf/timestamp.proto\";",
                      "message Coverage {",
                      "  google.protobuf.Timestamp ts = 1;",
                      "  repeated google.protobuf.Timestamp ts_list = 2;",
                      "}"))
              .toDescriptor();

  // ---- helpers -------------------------------------------------------------------------------

  private static RowType row(RowField... fields) {
    return new RowType(List.of(fields));
  }

  private static RowField field(String name, LogicalType type) {
    return new RowField(name, type);
  }

  private static Message write(RowType type, RowData row) {
    return (Message) RowDataToProtoConverters.createConverter(type, COVERAGE).convert(row);
  }

  private static RowData read(RowType type, Message message) throws Exception {
    final DynamicMessage parsed = DynamicMessage.parseFrom(COVERAGE, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(COVERAGE, type).convert(parsed);
  }

  private static Object protoField(Message message, String name) {
    return message.getField(message.getDescriptorForType().findFieldByName(name));
  }

  private static FieldDescriptor derivedField(RowType type, String name) {
    return RowTypeToProto.fromRowType(type, "Row", "derived").findFieldByName(name);
  }

  // ---- google.protobuf.Timestamp as TIMESTAMP(p) --------------------------------------------

  @Test
  void timestampWithoutTimeZoneRoundTripsAsUtc() throws Exception {
    final RowType type = row(field("ts", new TimestampType(9)));
    final LocalDateTime wallClock = LocalDateTime.of(2024, 3, 10, 2, 30, 15, 123_456_789);
    final GenericRowData in = GenericRowData.of(TimestampData.fromLocalDateTime(wallClock));

    final Message message = write(type, in);
    final Message ts = (Message) protoField(message, "ts");
    // 2024-03-10T02:30:15.123456789 read as UTC.
    assertEquals(1710037815L, protoField(ts, "seconds"));
    assertEquals(123_456_789, protoField(ts, "nanos"));

    final RowData out = read(type, message);
    assertEquals(wallClock, out.getTimestamp(0, 9).toLocalDateTime());
  }

  @Test
  void timestampWithoutTimeZoneReadsBeforeTheEpochAndUnsetAsNull() throws Exception {
    final RowType type = row(field("ts", new TimestampType(3)));
    final Message message =
        DynamicMessage.newBuilder(COVERAGE)
            .setField(
                COVERAGE.findFieldByName("ts"),
                Timestamp.newBuilder().setSeconds(-1).setNanos(500_000_000).build())
            .build();
    assertEquals(
        LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000),
        read(type, message).getTimestamp(0, 3).toLocalDateTime());

    assertNull(read(type, DynamicMessage.getDefaultInstance(COVERAGE)).getTimestamp(0, 3));
  }

  @Test
  void timestampWithoutTimeZoneInAnArrayRoundTrips() throws Exception {
    final RowType type =
        row(field("ts_list", new ArrayType(new TimestampType(6))));
    final LocalDateTime wallClock = LocalDateTime.of(2000, 1, 1, 0, 0, 0, 1_000);
    final GenericRowData in =
        GenericRowData.of(
            new GenericArrayData(
                new Object[] {TimestampData.fromLocalDateTime(wallClock)}));
    final RowData out = read(type, write(type, in));
    assertEquals(wallClock, out.getArray(0).getTimestamp(0, 6).toLocalDateTime());
  }

  @Test
  void timestampWithoutTimeZonePassesTheMessageClassCheck() {
    SourceMessageValidator.validateRowType(
        COVERAGE, row(field("ts", new TimestampType(3))), CLASS_NAME);
  }

  @Test
  void timestampWithoutTimeZoneDerivesAGoogleProtobufTimestamp() {
    final FieldDescriptor ts = derivedField(row(field("ts", new TimestampType(3))), "ts");
    assertEquals("google.protobuf.Timestamp", ts.getMessageType().getFullName());
  }

  @Test
  void theCheckStillRejectsATimestampForAScalarField() {
    final Descriptor scalar =
        (Descriptor)
            new ProtobufSchema("syntax = \"proto3\"; message S { int64 ts = 1; }").toDescriptor();
    assertThrows(
        ValidationException.class,
        () ->
            SourceMessageValidator.validateRowType(
                scalar, row(field("ts", new TimestampType(3))), CLASS_NAME));
  }
}
