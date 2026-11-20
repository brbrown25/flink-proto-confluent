package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.RowDataToProtoConverters;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * A {@code oneof} column's fields are matched to the oneof's cases by name, as the sink matches
 * them and as other columns are matched to fields (issue #231). A field that names no case falls
 * back to the case at its position, as before, when no other field of the column names that case.
 */
class OneofCaseMatchingTest {

  private static final String CLASS_NAME = "com.example.Event";

  private static final Descriptor EVENT =
      (Descriptor)
          new ProtobufSchema(
                  String.join(
                      "\n",
                      "syntax = \"proto3\";",
                      "package oneofs;",
                      "message Event {",
                      "  string id = 1;",
                      "  oneof payload {",
                      "    string first_name = 2;",
                      "    string last_name = 3;",
                      "    int64 count = 4;",
                      "    string nickName = 5;",
                      "  }",
                      "}"))
              .toDescriptor();

  private static final DataType REORDERED =
      ROW(
          FIELD("id", STRING()),
          FIELD(
              "payload",
              ROW(
                  FIELD("last_name", STRING()),
                  FIELD("count", BIGINT()),
                  FIELD("first_name", STRING()))));

  // ---- helpers -------------------------------------------------------------------------------

  private static RowType row(DataType type) {
    return (RowType) type.getLogicalType();
  }

  private static Message event(String caseName, Object value) {
    return DynamicMessage.newBuilder(EVENT)
        .setField(EVENT.findFieldByName("id"), "e-1")
        .setField(EVENT.findFieldByName(caseName), value)
        .build();
  }

  private static RowData read(DataType type, Message message) throws Exception {
    final DynamicMessage parsed = DynamicMessage.parseFrom(EVENT, message.toByteArray());
    return (RowData) ProtoToRowDataConverters.createConverter(EVENT, row(type)).convert(parsed);
  }

  private static RowData payload(DataType type, Message message) throws Exception {
    final RowData row = read(type, message);
    final int index = row(type).getFieldNames().indexOf("payload");
    final int arity = row(type).getTypeAt(index).getChildren().size();
    return row.isNullAt(index) ? null : row.getRow(index, arity);
  }

  private static String validationError(DataType type) {
    return assertThrows(
            ValidationException.class,
            () -> SourceMessageValidator.validateRowType(EVENT, row(type), CLASS_NAME))
        .getMessage();
  }

  // ---- full reads ----------------------------------------------------------------------------

  @Test
  void reorderedCasesAreReadByName() throws Exception {
    RowData payload = payload(REORDERED, event("first_name", "Ann"));
    assertTrue(payload.isNullAt(0));
    assertTrue(payload.isNullAt(1));
    assertEquals("Ann", payload.getString(2).toString());

    payload = payload(REORDERED, event("count", 7L));
    assertTrue(payload.isNullAt(0));
    assertEquals(7L, payload.getLong(1));
    assertTrue(payload.isNullAt(2));

    payload = payload(REORDERED, event("last_name", "Lee"));
    assertEquals("Lee", payload.getString(0).toString());
    assertTrue(payload.isNullAt(1));
    assertTrue(payload.isNullAt(2));
  }

  @Test
  void aSubsetOfTheCasesReadsOnlyThoseCases() throws Exception {
    DataType subset =
        ROW(FIELD("id", STRING()), FIELD("payload", ROW(FIELD("last_name", STRING()))));

    assertEquals("Lee", payload(subset, event("last_name", "Lee")).getString(0).toString());
    // Another case is set: the column is not NULL (a case is set) but its only field is.
    RowData payload = payload(subset, event("first_name", "Ann"));
    assertTrue(payload.isNullAt(0));
  }

  @Test
  void aSnakeCaseFieldReadsACamelCaseCase() throws Exception {
    DataType type =
        ROW(
            FIELD("id", STRING()),
            FIELD("payload", ROW(FIELD("count", BIGINT()), FIELD("nick_name", STRING()))));

    RowData payload = payload(type, event("nickName", "Al"));
    assertTrue(payload.isNullAt(0));
    assertEquals("Al", payload.getString(1).toString());
    assertDoesNotThrow(() -> SourceMessageValidator.validateRowType(EVENT, row(type), CLASS_NAME));
  }

  @Test
  void fieldsNamingNoCaseStillReadTheCaseAtTheirPosition() throws Exception {
    // A table written before cases were matched by name keeps reading them by position.
    DataType positional =
        ROW(
            FIELD("id", STRING()),
            FIELD(
                "payload",
                ROW(FIELD("x", STRING()), FIELD("y", STRING()), FIELD("z", BIGINT()))));

    assertEquals("Ann", payload(positional, event("first_name", "Ann")).getString(0).toString());
    assertEquals("Lee", payload(positional, event("last_name", "Lee")).getString(1).toString());
    assertEquals(7L, payload(positional, event("count", 7L)).getLong(2));
    assertDoesNotThrow(
        () -> SourceMessageValidator.validateRowType(EVENT, row(positional), CLASS_NAME));

    // Mixed: 'surname' names no case and its position's case (last_name) is not named by another
    // field, so it reads last_name as before.
    DataType mixed =
        ROW(
            FIELD("id", STRING()),
            FIELD("payload", ROW(FIELD("first_name", STRING()), FIELD("surname", STRING()))));
    RowData payload = payload(mixed, event("last_name", "Lee"));
    assertTrue(payload.isNullAt(0));
    assertEquals("Lee", payload.getString(1).toString());
    assertDoesNotThrow(() -> SourceMessageValidator.validateRowType(EVENT, row(mixed), CLASS_NAME));
  }

  @Test
  void aFieldNamingNoCaseWhosePositionIsTakenReadsNull() throws Exception {
    // 'other' is at position 0, whose case (first_name) another field names.
    DataType type =
        ROW(
            FIELD("id", STRING()),
            FIELD("payload", ROW(FIELD("other", STRING()), FIELD("first_name", STRING()))));

    RowData payload = payload(type, event("first_name", "Ann"));
    assertTrue(payload.isNullAt(0));
    assertEquals("Ann", payload.getString(1).toString());

    String message = validationError(type);
    assertTrue(
        message.contains("column 'payload.other' has no case in oneof 'oneofs.Event.payload'"),
        message);
    assertTrue(message.contains("first_name, last_name, count, nickName"), message);
  }

  // ---- message-class check -------------------------------------------------------------------

  @Test
  void messageClassCheckMatchesCasesByName() {
    assertDoesNotThrow(
        () -> SourceMessageValidator.validateRowType(EVENT, row(REORDERED), CLASS_NAME));

    // The type is checked against the case of the field's name, not the case at its position.
    String message =
        validationError(
            ROW(
                FIELD("id", STRING()),
                FIELD("payload", ROW(FIELD("count", STRING()), FIELD("first_name", BIGINT())))));
    assertTrue(message.contains("column 'payload.count' is STRING"), message);
    assertTrue(message.contains("column 'payload.first_name' is BIGINT"), message);

    // Past the last case, a field that names no case has none to fall back to.
    message =
        validationError(
            ROW(
                FIELD("id", STRING()),
                FIELD(
                    "payload",
                    ROW(
                        FIELD("first_name", STRING()),
                        FIELD("last_name", STRING()),
                        FIELD("count", BIGINT()),
                        FIELD("nickName", STRING()),
                        FIELD("nope", STRING())))));
    assertTrue(message.contains("column 'payload.nope' has no case in oneof"), message);
  }

  // ---- nested projection ---------------------------------------------------------------------

  @Test
  void projectingOneCaseOfAReorderedOneofReadsThatCase() throws Exception {
    int[][] projection = {{1, 2}, {1, 1}};
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateProjection(
                EVENT, row(REORDERED), projection, CLASS_NAME));
    ProtoToRowDataConverters.ProtoToRowDataConverter converter =
        ProtoToRowDataConverters.createProjectedConverter(EVENT, row(REORDERED), projection);

    RowData row = (RowData) converter.convert(event("first_name", "Ann"));
    assertEquals("Ann", row.getString(0).toString());
    assertTrue(row.isNullAt(1));

    row = (RowData) converter.convert(event("count", 7L));
    assertTrue(row.isNullAt(0));
    assertEquals(7L, row.getLong(1));
  }

  @Test
  void projectingAFieldWhosePositionIsTakenIsReportedAndReadsNull() throws Exception {
    DataType type =
        ROW(
            FIELD("id", STRING()),
            FIELD("payload", ROW(FIELD("other", STRING()), FIELD("first_name", STRING()))));
    int[][] projection = {{1, 0}};

    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                SourceMessageValidator.validateProjection(
                    EVENT, row(type), projection, CLASS_NAME));
    assertTrue(
        e.getMessage().contains("column 'payload.other' has no case in oneof"), e.getMessage());

    RowData row =
        (RowData)
            ProtoToRowDataConverters.createProjectedConverter(EVENT, row(type), projection)
                .convert(event("first_name", "Ann"));
    assertTrue(row.isNullAt(0));
  }

  // ---- write, then read ----------------------------------------------------------------------

  @Test
  void aReorderedOneofRoundTrips() throws Exception {
    RowType type = row(REORDERED);
    GenericRowData payload = GenericRowData.of(null, null, StringData.fromString("Ann"));
    GenericRowData written = GenericRowData.of(StringData.fromString("e-1"), payload);

    Message message =
        (Message) RowDataToProtoConverters.createConverter(type, EVENT).convert(written);
    assertEquals("Ann", message.getField(EVENT.findFieldByName("first_name")));
    assertFalse(message.hasField(EVENT.findFieldByName("last_name")));

    RowData readBack = payload(REORDERED, message);
    assertTrue(readBack.isNullAt(0));
    assertTrue(readBack.isNullAt(1));
    assertEquals("Ann", readBack.getString(2).toString());
  }
}
