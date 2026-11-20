package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.type.Date;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.time.LocalDate;
import java.util.List;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.utils.DateTimeUtils;
import org.junit.jupiter.api.Test;

/**
 * Reading a {@code google.type.Date} into a {@code DATE} column (issue #236). The message allows 0
 * in each part: all zeros is an empty date, which reads as {@code NULL}; a year, month or day of 0
 * alone is a partial date, which no {@code DATE} can hold, so it fails the record with an error that
 * names the field and the value instead of a bare {@code DateTimeException}.
 */
class DateZeroPartsTest {

  private static final Descriptor DATES =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package dates;",
                      "import \"google/type/date.proto\";",
                      "message Dates {",
                      "  google.type.Date d = 1;",
                      "  repeated google.type.Date ds = 2;",
                      "  Inner inner = 3;",
                      "}",
                      "message Inner {",
                      "  google.type.Date d = 1;",
                      "}"))
              .toDescriptor();

  private static final Descriptor INNER = DATES.findFieldByName("inner").getMessageType();

  private static RowType row(RowField... fields) {
    return new RowType(List.of(fields));
  }

  private static RowField field(String name, LogicalType type) {
    return new RowField(name, type);
  }

  private static RowData read(RowType type, Message message) throws Exception {
    final DynamicMessage parsed = DynamicMessage.parseFrom(DATES, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(DATES, type).convert(parsed);
  }

  private static Date date(int year, int month, int day) {
    return Date.newBuilder().setYear(year).setMonth(month).setDay(day).build();
  }

  private static Message withDate(Date date) {
    return DynamicMessage.newBuilder(DATES).setField(DATES.findFieldByName("d"), date).build();
  }

  private static int epochDay(int year, int month, int day) {
    return DateTimeUtils.toInternal(LocalDate.of(year, month, day));
  }

  private static final RowType DATE_ROW = row(field("d", new DateType()));

  private static IllegalArgumentException readFails(Date date) {
    return assertThrows(IllegalArgumentException.class, () -> read(DATE_ROW, withDate(date)));
  }

  @Test
  void aFullDateReadsAsThatDay() throws Exception {
    assertEquals(epochDay(2024, 2, 29), read(DATE_ROW, withDate(date(2024, 2, 29))).getInt(0));
  }

  @Test
  void anUnsetDateReadsAsNull() throws Exception {
    assertTrue(read(DATE_ROW, DynamicMessage.getDefaultInstance(DATES)).isNullAt(0));
  }

  @Test
  void aPresentButEmptyDateReadsAsNull() throws Exception {
    final Message message = withDate(Date.getDefaultInstance());
    // The empty message is on the wire, so the field is set: only the zero parts make it NULL.
    assertTrue(
        DynamicMessage.parseFrom(DATES, message.toByteArray())
            .hasField(DATES.findFieldByName("d")));
    assertTrue(read(DATE_ROW, message).isNullAt(0));
  }

  @Test
  void aDateWithoutAYearFailsNamingTheField() {
    final IllegalArgumentException e = readFails(date(0, 2, 29));
    assertTrue(e.getMessage().contains("dates.Dates.d"), e.getMessage());
    assertTrue(e.getMessage().contains("year 0, month 2, day 29"), e.getMessage());
  }

  @Test
  void aDateWithoutAYearOrMonthFailsNamingTheField() {
    final IllegalArgumentException e = readFails(date(0, 0, 15));
    assertTrue(e.getMessage().contains("dates.Dates.d"), e.getMessage());
    assertTrue(e.getMessage().contains("year 0, month 0, day 15"), e.getMessage());
  }

  @Test
  void aDateWithoutAMonthFailsNamingTheField() {
    final IllegalArgumentException e = readFails(date(2024, 0, 0));
    assertTrue(e.getMessage().contains("dates.Dates.d"), e.getMessage());
    assertTrue(e.getMessage().contains("year 2024, month 0, day 0"), e.getMessage());
  }

  @Test
  void aDateWithoutADayFailsNamingTheField() {
    final IllegalArgumentException e = readFails(date(2024, 5, 0));
    assertTrue(e.getMessage().contains("dates.Dates.d"), e.getMessage());
    assertTrue(e.getMessage().contains("year 2024, month 5, day 0"), e.getMessage());
  }

  @Test
  void aDayThatDoesNotExistFailsNamingTheField() {
    final IllegalArgumentException e = readFails(date(2023, 2, 30));
    assertTrue(e.getMessage().contains("dates.Dates.d"), e.getMessage());
    assertTrue(e.getMessage().contains("year 2023, month 2, day 30"), e.getMessage());
  }

  @Test
  void aRowColumnReadsAPartialDateAsItsParts() throws Exception {
    final RowType type =
        row(
            field(
                "d",
                row(
                    field("year", new IntType()),
                    field("month", new IntType()),
                    field("day", new IntType()))));
    final RowData parts = read(type, withDate(date(0, 12, 25))).getRow(0, 3);
    assertEquals(0, parts.getInt(0));
    assertEquals(12, parts.getInt(1));
    assertEquals(25, parts.getInt(2));
  }

  @Test
  void aNestedDateReadsTheSameWay() throws Exception {
    final RowType type = row(field("inner", row(field("d", new DateType()))));
    final Message empty =
        DynamicMessage.newBuilder(DATES)
            .setField(
                DATES.findFieldByName("inner"),
                DynamicMessage.newBuilder(INNER)
                    .setField(INNER.findFieldByName("d"), Date.getDefaultInstance())
                    .build())
            .build();
    assertTrue(read(type, empty).getRow(0, 1).isNullAt(0));

    final Message partial =
        DynamicMessage.newBuilder(DATES)
            .setField(
                DATES.findFieldByName("inner"),
                DynamicMessage.newBuilder(INNER)
                    .setField(INNER.findFieldByName("d"), date(2024, 0, 0))
                    .build())
            .build();
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> read(type, partial));
    assertTrue(e.getMessage().contains("dates.Inner.d"), e.getMessage());
  }

  @Test
  void anEmptyDateInARepeatedFieldReadsAsANullElement() throws Exception {
    final RowType type = row(field("ds", new ArrayType(new DateType())));
    final Message message =
        DynamicMessage.newBuilder(DATES)
            .addRepeatedField(DATES.findFieldByName("ds"), date(2024, 1, 2))
            .addRepeatedField(DATES.findFieldByName("ds"), Date.getDefaultInstance())
            .build();
    final ArrayData array = read(type, message).getArray(0);
    assertEquals(2, array.size());
    assertFalse(array.isNullAt(0));
    assertEquals(epochDay(2024, 1, 2), array.getInt(0));
    assertTrue(array.isNullAt(1));
  }

  @Test
  void aPartialDateInARepeatedFieldFailsNamingTheField() {
    final RowType type = row(field("ds", new ArrayType(new DateType())));
    final Message message =
        DynamicMessage.newBuilder(DATES)
            .addRepeatedField(DATES.findFieldByName("ds"), date(0, 12, 25))
            .build();
    final IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> read(type, message));
    assertTrue(e.getMessage().contains("dates.Dates.ds"), e.getMessage());
  }
}
