package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestEnums;
import com.bbrownsound.flink.formats.proto.test.v1.TestMap;
import com.bbrownsound.flink.formats.proto.test.v1.TestOneof;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeatedNested;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.bbrownsound.flink.formats.proto.test.v1.TestWrapperTypes;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Struct;
import java.util.stream.Stream;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class SourceMessageValidatorTest {

  private static final String CLASS_NAME = "com.example.Proto$Msg";

  private static RowType row(DataType type) {
    return (RowType) type.getLogicalType();
  }

  private static String validationError(Descriptor descriptor, DataType rowType) {
    return assertThrows(
            ValidationException.class,
            () -> SourceMessageValidator.validateRowType(descriptor, row(rowType), CLASS_NAME))
        .getMessage();
  }

  // ---- message-name syntax ----

  @ParameterizedTest
  @ValueSource(strings = {"Order", "com.example.Order", "com.example.Order.Line", "a_b.C1", "_x"})
  void validateMessageName_acceptsFullyQualifiedNames(String name) {
    assertDoesNotThrow(() -> SourceMessageValidator.validateMessageName("message-name", name));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        " com.example.Order",
        ".com.example.Order",
        "com..Order",
        "com.example.",
        "1abc",
        "com.example.Proto$Order",
        "com/example/Order"
      })
  void validateMessageName_rejectsAnythingElse(String name) {
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> SourceMessageValidator.validateMessageName("message-name", name));
    assertTrue(e.getMessage().contains("'message-name'"), e.getMessage());
    assertTrue(e.getMessage().contains("'" + name + "'"), e.getMessage());
  }

  @Test
  void validateMessageName_rejectsNull() {
    assertThrows(
        ValidationException.class,
        () -> SourceMessageValidator.validateMessageName("message-name", null));
  }

  // ---- loading message-class ----

  @Test
  void loadDescriptor_returnsTheGeneratedMessageDescriptor() throws Exception {
    Descriptor descriptor =
        SourceMessageValidator.loadDescriptor(
            TestSimple.SimpleMessage.class.getName(), getClass().getClassLoader());
    assertEquals("com.bbrownsound.flink.formats.proto.test.v1.SimpleMessage",
        descriptor.getFullName());
  }

  @Test
  void loadDescriptor_missingClass_throwsClassNotFound() {
    assertThrows(
        ClassNotFoundException.class,
        () -> SourceMessageValidator.loadDescriptor("com.example.Nope", getClass().getClassLoader()));
  }

  @Test
  void loadDescriptor_notAMessageClass_isRejected() {
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                SourceMessageValidator.loadDescriptor(
                    String.class.getName(), getClass().getClassLoader()));
    assertTrue(e.getMessage().contains("not a generated protobuf message class"), e.getMessage());
  }

  @Test
  void loadDescriptor_messageWithoutAStaticDefaultInstance_isRejected() {
    // DynamicMessage is a Message but has no no-arg static getDefaultInstance().
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                SourceMessageValidator.loadDescriptor(
                    DynamicMessage.class.getName(), getClass().getClassLoader()));
    assertTrue(e.getMessage().contains("Failed to read the descriptor"), e.getMessage());
    assertTrue(e.getMessage().contains(DynamicMessage.class.getName()), e.getMessage());
  }

  // ---- expected message name ----

  @Test
  void expectedMessageName_withoutClass_isMessageName() {
    assertEquals("a.B", SourceMessageValidator.expectedMessageName("a.B", null, null));
    assertNull(SourceMessageValidator.expectedMessageName(null, null, null));
  }

  @Test
  void expectedMessageName_withClass_isDescriptorName() {
    Descriptor d = TestSimple.SimpleMessage.getDescriptor();
    assertEquals(d.getFullName(), SourceMessageValidator.expectedMessageName(null, "C", d));
    assertEquals(
        d.getFullName(), SourceMessageValidator.expectedMessageName(d.getFullName(), "C", d));
  }

  @Test
  void expectedMessageName_conflictingNameAndClass_isRejected() {
    Descriptor d = TestSimple.SimpleMessage.getDescriptor();
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> SourceMessageValidator.expectedMessageName("com.example.Order", CLASS_NAME, d));
    assertTrue(e.getMessage().contains("'message-name' = 'com.example.Order'"), e.getMessage());
    assertTrue(e.getMessage().contains(CLASS_NAME), e.getMessage());
    assertTrue(e.getMessage().contains(d.getFullName()), e.getMessage());
  }

  // ---- row type validation ----

  static Stream<Descriptor> testMessages() {
    return Stream.of(
        TestSimple.SimpleMessage.getDescriptor(),
        TestBig.BigPbMessage.getDescriptor(),
        TestMap.MapTest.getDescriptor(),
        TestOneof.MessageWithOneof.getDescriptor(),
        TestRepeated.RepeatedTest.getDescriptor(),
        TestRepeatedNested.RepeatedMessageTest.getDescriptor(),
        TestWrapperTypes.WrapperTypesTest.getDescriptor(),
        TestEnums.TestEnum.getDescriptor());
  }

  @ParameterizedTest
  @MethodSource("testMessages")
  void validateRowType_theDerivedRowTypeMatches(Descriptor descriptor) {
    RowType derived = (RowType) ProtoToLogicalType.toLogicalType(descriptor);
    assertDoesNotThrow(
        () -> SourceMessageValidator.validateRowType(descriptor, derived, CLASS_NAME));
  }

  @Test
  void validateRowType_aSubsetOfFieldsMatches() {
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                TestBig.BigPbMessage.getDescriptor(),
                row(ROW(FIELD("int64_field_8", BIGINT()), FIELD("string_field_3", STRING()))),
                CLASS_NAME));
  }

  @Test
  void validateRowType_lengthAndIntWidthVariantsMatch() {
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                TestBig.BigPbMessage.getDescriptor(),
                row(
                    ROW(
                        FIELD("int_field_1", SMALLINT()),
                        FIELD("int32_field_16", TINYINT()),
                        FIELD("string_field_3", CHAR(5)),
                        FIELD("bytes_field_4", BYTES()),
                        FIELD("bytes_field_12", BINARY(16)),
                        FIELD("bytes_field_10", VARBINARY(10)))),
                CLASS_NAME));
  }

  @Test
  void validateRowType_snakeCaseColumnMatchesCamelCaseField() {
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                TestWrapperTypes.WrapperTypesTest.getDescriptor(),
                row(ROW(FIELD("int32_value", INT()), FIELD("string_value", STRING()))),
                CLASS_NAME));
  }

  @Test
  void validateRowType_missingColumn_namesTheColumnAndTheMessageFields() {
    String message =
        validationError(
            TestSimple.SimpleMessage.getDescriptor(),
            ROW(FIELD("content", STRING()), FIELD("order_id", STRING())));
    assertTrue(message.contains("column 'order_id' has no field"), message);
    assertTrue(message.contains("SimpleMessage"), message);
    assertTrue(message.contains("fields: content, date_time"), message);
    assertTrue(message.contains("'message-class' = '" + CLASS_NAME + "'"), message);
  }

  @Test
  void validateRowType_incompatibleType_namesTheColumnAndTheFieldType() {
    String message =
        validationError(TestSimple.SimpleMessage.getDescriptor(), ROW(FIELD("content", INT())));
    assertTrue(message.contains("column 'content' is INT"), message);
    assertTrue(message.contains("(string) reads as STRING"), message);
  }

  @Test
  void validateRowType_unsignedIntIsNotAnInt() {
    String message =
        validationError(TestBig.BigPbMessage.getDescriptor(), ROW(FIELD("uint32_field_7", INT())));
    assertTrue(message.contains("column 'uint32_field_7' is INT"), message);
    assertTrue(message.contains("(uint32) reads as BIGINT"), message);
  }

  @Test
  void validateRowType_reportsEveryProblem() {
    String message =
        validationError(
            TestSimple.SimpleMessage.getDescriptor(),
            ROW(FIELD("content", INT()), FIELD("a", STRING()), FIELD("b", STRING())));
    assertTrue(message.contains("column 'content'"), message);
    assertTrue(message.contains("column 'a'"), message);
    assertTrue(message.contains("column 'b'"), message);
  }

  @Test
  void validateRowType_nestedMissingColumn_usesTheColumnPath() {
    String message =
        validationError(
            TestRepeatedNested.RepeatedMessageTest.getDescriptor(),
            ROW(FIELD("d", ROW(FIELD("a", INT()), FIELD("zz", INT())))));
    assertTrue(message.contains("column 'd.zz' has no field"), message);
    assertTrue(message.contains("InnerMessageTest"), message);
  }

  @Test
  void validateRowType_messageFieldDeclaredAsScalar_isRejected() {
    String message =
        validationError(
            TestRepeatedNested.RepeatedMessageTest.getDescriptor(), ROW(FIELD("d", STRING())));
    assertTrue(message.contains("column 'd' is STRING"), message);
    assertTrue(message.contains("reads as ROW"), message);
  }

  @Test
  void validateRowType_wrapperFieldMayBeReadAsItsRow() {
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                TestWrapperTypes.WrapperTypesTest.getDescriptor(),
                row(ROW(FIELD("stringValue", ROW(FIELD("value", STRING()))))),
                CLASS_NAME));
  }

  @Test
  void validateRowType_repeatedFieldsNeedArrays() {
    String message =
        validationError(TestRepeated.RepeatedTest.getDescriptor(), ROW(FIELD("values", STRING())));
    assertTrue(message.contains("column 'values' is STRING"), message);
    assertTrue(message.contains("(repeated string) reads as an ARRAY"), message);

    message =
        validationError(
            TestRepeated.RepeatedTest.getDescriptor(), ROW(FIELD("numbers", ARRAY(STRING()))));
    assertTrue(message.contains("column 'numbers[]' is STRING"), message);

    message =
        validationError(
            TestSimple.SimpleMessage.getDescriptor(), ROW(FIELD("content", ARRAY(STRING()))));
    assertTrue(message.contains("column 'content' is ARRAY<STRING>"), message);
  }

  @Test
  void validateRowType_repeatedNestedAndTimestamps() {
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                TestRepeated.RepeatedTest.getDescriptor(),
                row(
                    ROW(
                        FIELD("timestamps", ARRAY(TIMESTAMP_LTZ(3))),
                        FIELD("nested_repeated", ARRAY(ROW(FIELD("value", STRING())))))),
                CLASS_NAME));
    String message =
        validationError(
            TestRepeated.RepeatedTest.getDescriptor(),
            ROW(FIELD("timestamps", ARRAY(TIMESTAMP(3)))));
    assertTrue(message.contains("column 'timestamps[]' is TIMESTAMP(3)"), message);
    assertTrue(message.contains("google.protobuf.Timestamp"), message);
  }

  @Test
  void validateRowType_maps() {
    Descriptor d = TestMap.MapTest.getDescriptor();
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                d,
                row(
                    ROW(
                        FIELD("simple_string_map", MAP(STRING(), STRING())),
                        FIELD("nested_map", MAP(STRING(), ROW(FIELD("id", STRING())))))),
                CLASS_NAME));
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                d, row(ROW(FIELD("simple_string_map", MULTISET(STRING())))), CLASS_NAME));

    String message = validationError(d, ROW(FIELD("simple_string_map", MAP(STRING(), INT()))));
    assertTrue(message.contains("column 'simple_string_map.value' is INT"), message);

    message = validationError(d, ROW(FIELD("simple_string_map", MAP(INT(), STRING()))));
    assertTrue(message.contains("column 'simple_string_map.key' is INT"), message);

    message =
        validationError(d, ROW(FIELD("nested_map", MAP(STRING(), ROW(FIELD("nope", STRING()))))));
    assertTrue(message.contains("column 'nested_map.value.nope' has no field"), message);

    message = validationError(d, ROW(FIELD("simple_string_map", STRING())));
    assertTrue(message.contains("reads as a MAP"), message);
  }

  @Test
  void validateRowType_oneofsMatchTheirCasesByPosition() {
    Descriptor d = TestOneof.MessageWithOneof.getDescriptor();
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                d, row(ROW(FIELD("choice", ROW(FIELD("x", INT()), FIELD("y", STRING()))))),
                CLASS_NAME));

    String message =
        validationError(
            d, ROW(FIELD("choice", ROW(FIELD("a", INT()), FIELD("b", STRING()), FIELD("c", INT())))));
    assertTrue(message.contains("column 'choice.c' has no case in oneof"), message);

    message = validationError(d, ROW(FIELD("choice", ROW(FIELD("a", STRING())))));
    assertTrue(message.contains("column 'choice.a' is STRING"), message);

    message = validationError(d, ROW(FIELD("choice", STRING())));
    assertTrue(message.contains("column 'choice' is STRING but oneof"), message);

    // A oneof case is not addressable by its own name at the top level.
    message = validationError(d, ROW(FIELD("a", INT())));
    assertTrue(message.contains("column 'a' has no field"), message);
    assertTrue(message.contains("fields: choice"), message);
  }

  @Test
  void validateRowType_cyclicMessageDeclaredAsScalar_readsAsRow() {
    // google.protobuf.Struct -> map<string, Value> -> Value.struct_value -> Struct is cyclic, so
    // the field's Flink type cannot be derived; a non-ROW column for it is still a mismatch.
    String message =
        validationError(Struct.getDescriptor(), ROW(FIELD("fields", MAP(STRING(), STRING()))));
    assertTrue(message.contains("column 'fields.value' is STRING"), message);
    assertTrue(message.contains("(google.protobuf.Value) reads as a ROW"), message);
  }

  @Test
  void validateRowType_enumsReadAsStrings() {
    Descriptor d = TestEnums.TestEnum.getDescriptor();
    assertDoesNotThrow(
        () ->
            SourceMessageValidator.validateRowType(
                d, row(ROW(FIELD("type", STRING()), FIELD("status", ARRAY(STRING())))),
                CLASS_NAME));
    String message = validationError(d, ROW(FIELD("type", INT())));
    assertTrue(message.contains("(enum) reads as STRING"), message);
  }
}
