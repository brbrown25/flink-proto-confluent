package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoToRowDataConverters;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

/**
 * The sink writes a {@code TIMESTAMP} or {@code TIMESTAMP_LTZ} before 1970 as a valid {@code
 * google.protobuf.Timestamp}: {@code seconds} rounds toward negative infinity and {@code nanos}
 * stays in 0 to 999,999,999 (issue #230).
 */
class PreEpochTimestampWriteTest {

  private static final Descriptor EVENT =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package preepoch;",
                      "import \"google/protobuf/timestamp.proto\";",
                      "message Event {",
                      "  google.protobuf.Timestamp ts = 1;",
                      "}"))
              .toDescriptor();

  private static final RowType TIMESTAMP = row(new TimestampType(9));

  private static final RowType TIMESTAMP_LTZ = row(new LocalZonedTimestampType(9));

  private static RowType row(LogicalType type) {
    return new RowType(List.of(new RowField("ts", type)));
  }

  private static Message write(RowType type, TimestampData value) {
    return (Message)
        RowDataToProtoConverters.createConverter(type, EVENT).convert(GenericRowData.of(value));
  }

  private static Timestamp writtenTimestamp(RowType type, TimestampData value) throws Exception {
    final Message message = write(type, value);
    final Message ts = (Message) message.getField(EVENT.findFieldByName("ts"));
    return Timestamp.parseFrom(ts.toByteString());
  }

  private static TimestampData read(RowType type, Message message) throws Exception {
    final DynamicMessage parsed = DynamicMessage.parseFrom(EVENT, message.toByteArray());
    final RowData row =
        (RowData) ProtoToRowDataConverters.createConverter(EVENT, type).convert(parsed);
    return row.getTimestamp(0, 9);
  }

  private static void assertWritten(
      RowType type, TimestampData value, long expectedSeconds, int expectedNanos)
      throws Exception {
    final Timestamp ts = writtenTimestamp(type, value);
    assertTrue(Timestamps.isValid(ts), "invalid google.protobuf.Timestamp: " + ts);
    assertEquals(expectedSeconds, ts.getSeconds(), "seconds of " + value);
    assertEquals(expectedNanos, ts.getNanos(), "nanos of " + value);
  }

  @Test
  void aTimestampHalfASecondBeforeTheEpochIsWrittenWithNonNegativeNanos() throws Exception {
    assertWritten(
        TIMESTAMP,
        TimestampData.fromLocalDateTime(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000)),
        -1L,
        500_000_000);
  }

  @Test
  void aPreEpochTimestampKeepsItsNanosBelowAMillisecond() throws Exception {
    assertWritten(
        TIMESTAMP,
        TimestampData.fromLocalDateTime(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 123_456_789)),
        -1L,
        123_456_789);
    assertWritten(
        TIMESTAMP,
        TimestampData.fromLocalDateTime(LocalDateTime.of(1900, 6, 15, 12, 0, 0, 999_999_999)),
        LocalDateTime.of(1900, 6, 15, 12, 0, 0).toEpochSecond(ZoneOffset.UTC),
        999_999_999);
  }

  @Test
  void secondBoundariesAroundTheEpochAreWrittenExactly() throws Exception {
    assertWritten(TIMESTAMP, TimestampData.fromEpochMillis(0L), 0L, 0);
    assertWritten(TIMESTAMP, TimestampData.fromEpochMillis(-1_000L), -1L, 0);
    assertWritten(TIMESTAMP, TimestampData.fromEpochMillis(-1L), -1L, 999_000_000);
    assertWritten(TIMESTAMP, TimestampData.fromEpochMillis(-1_001L), -2L, 999_000_000);
    assertWritten(TIMESTAMP, TimestampData.fromEpochMillis(-1L, 999_999), -1L, 999_999_999);
    assertWritten(TIMESTAMP, TimestampData.fromEpochMillis(1_000L), 1L, 0);
    assertWritten(TIMESTAMP, TimestampData.fromEpochMillis(1_500L), 1L, 500_000_000);
  }

  @Test
  void aTimestampWithLocalTimeZoneBeforeTheEpochIsWrittenWithNonNegativeNanos() throws Exception {
    assertWritten(
        TIMESTAMP_LTZ,
        TimestampData.fromInstant(Instant.parse("1969-12-31T23:59:59.500Z")),
        -1L,
        500_000_000);
    assertWritten(
        TIMESTAMP_LTZ,
        TimestampData.fromInstant(Instant.parse("1969-12-31T23:59:59.123456789Z")),
        -1L,
        123_456_789);
  }

  @Test
  void preEpochTimestampsRoundTripThroughTheWriterAndReader() throws Exception {
    final LocalDateTime[] wallClocks = {
      LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000),
      LocalDateTime.of(1969, 12, 31, 23, 59, 59, 1),
      LocalDateTime.of(1969, 12, 31, 23, 59, 59, 0),
      LocalDateTime.of(1955, 11, 5, 6, 15, 30, 987_654_321),
      LocalDateTime.of(1, 1, 1, 0, 0, 0, 1_000),
    };
    for (LocalDateTime wallClock : wallClocks) {
      final TimestampData in = TimestampData.fromLocalDateTime(wallClock);
      final Message timestamp = write(TIMESTAMP, in);
      assertTrue(
          Timestamps.isValid(writtenTimestamp(TIMESTAMP, in)), "invalid Timestamp for " + wallClock);
      assertEquals(wallClock, read(TIMESTAMP, timestamp).toLocalDateTime());

      final Instant instant = wallClock.toInstant(ZoneOffset.UTC);
      final TimestampData ltz = TimestampData.fromInstant(instant);
      assertTrue(
          Timestamps.isValid(writtenTimestamp(TIMESTAMP_LTZ, ltz)), "invalid Timestamp for " + instant);
      assertEquals(instant, read(TIMESTAMP_LTZ, write(TIMESTAMP_LTZ, ltz)).toInstant());
    }
  }
}
