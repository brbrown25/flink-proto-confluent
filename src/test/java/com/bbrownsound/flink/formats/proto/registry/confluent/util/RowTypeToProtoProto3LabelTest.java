package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * The descriptor derived from a table's row type is a proto3 file, and proto3 has no {@code
 * required} label (#237). {@code NOT NULL} columns become plain proto3 fields; nullable columns
 * keep proto3 {@code optional}. {@code NOT NULL} stays a Flink-side constraint.
 */
class RowTypeToProtoProto3LabelTest {

  private static final RowType ROW_TYPE =
      new RowType(
          false,
          List.of(
              new RowField("id", new BigIntType(false)),
              new RowField("name", new VarCharType(true, 10)),
              new RowField("tags", new ArrayType(false, new VarCharType(false, 10))),
              new RowField("bag", new MultisetType(true, new VarCharType(true, 10))),
              new RowField(
                  "attrs", new MapType(false, new VarCharType(false, 10), new IntType(false))),
              new RowField("inner", new RowType(false, List.of(new RowField("x", new IntType(false)))))));

  private static Descriptor derive() {
    return RowTypeToProto.fromRowType(ROW_TYPE, "Row", "io.confluent.generated");
  }

  @Test
  void notNullScalarIsPlainProto3Field() {
    FieldDescriptor id = derive().findFieldByName("id");
    assertFalse(id.isRequired());
    assertEquals(Label.LABEL_OPTIONAL, id.toProto().getLabel());
    assertFalse(id.toProto().getProto3Optional());
    assertFalse(id.hasPresence());
  }

  @Test
  void nullableScalarKeepsProto3Optional() {
    FieldDescriptor name = derive().findFieldByName("name");
    assertFalse(name.isRequired());
    assertTrue(name.toProto().getProto3Optional());
    assertTrue(name.hasPresence());
  }

  @Test
  void notNullArrayIsRepeated() {
    FieldDescriptor tags = derive().findFieldByName("tags");
    assertTrue(tags.isRepeated());
    assertFalse(tags.isRequired());
    assertFalse(tags.toProto().getProto3Optional());
  }

  @Test
  void notNullNestedRowIsPlainMessageField() {
    FieldDescriptor inner = derive().findFieldByName("inner");
    assertFalse(inner.isRequired());
    assertEquals(Label.LABEL_OPTIONAL, inner.toProto().getLabel());
    assertFalse(inner.toProto().getProto3Optional());
    FieldDescriptor x = inner.getMessageType().findFieldByName("x");
    assertFalse(x.isRequired());
    assertFalse(x.hasPresence());
  }

  @Test
  void mapAndMultisetEntryFieldsAreNotRequired() {
    Descriptor descriptor = derive();
    for (String field : List.of("bag", "attrs")) {
      Descriptor entry = descriptor.findFieldByName(field).getMessageType();
      for (FieldDescriptor entryField : entry.getFields()) {
        assertFalse(entryField.isRequired(), field + "." + entryField.getName());
      }
    }
  }

  @Test
  void noFieldAnywhereIsRequired() {
    Descriptor descriptor = derive();
    assertEquals("proto3", descriptor.getFile().toProto().getSyntax());
    List<String> required = new ArrayList<>();
    collectRequired(descriptor.toProto(), "", required);
    assertEquals(List.of(), required);
    // A message with required fields cannot be built until they are set; a proto3 message can.
    DynamicMessage empty = DynamicMessage.newBuilder(descriptor).build();
    assertTrue(empty.isInitialized());
  }

  @Test
  void schemaTextHasNoRequiredAndParsesAsProto3() {
    String text = new ProtobufSchema(derive()).canonicalString();
    assertEquals(
        "syntax = \"proto3\";\n"
            + "package io.confluent.generated;\n"
            + "\n"
            + "message Row {\n"
            + "  int64 id = 1;\n"
            + "  optional string name = 2;\n"
            + "  repeated string tags = 3;\n"
            + "  repeated BagEntry bag = 4;\n"
            + "  repeated AttrsEntry attrs = 5;\n"
            + "  inner_Row inner = 6;\n"
            + "\n"
            + "  message BagEntry {\n"
            + "    optional string key = 1;\n"
            + "    int32 value = 2;\n"
            + "  }\n"
            + "  message AttrsEntry {\n"
            + "    string key = 1;\n"
            + "    int32 value = 2;\n"
            + "  }\n"
            + "  message inner_Row {\n"
            + "    int32 x = 1;\n"
            + "  }\n"
            + "}\n",
        text);
    assertFalse(text.contains("required"), text);
    Descriptor reparsed = new ProtobufSchema(text).toDescriptor();
    assertEquals("proto3", reparsed.getFile().toProto().getSyntax());
    assertFalse(reparsed.findFieldByName("id").isRequired());
  }

  private static void collectRequired(DescriptorProto message, String prefix, List<String> out) {
    for (FieldDescriptorProto field : message.getFieldList()) {
      if (field.getLabel() == Label.LABEL_REQUIRED) {
        out.add(prefix + message.getName() + "." + field.getName());
      }
    }
    for (DescriptorProto nested : message.getNestedTypeList()) {
      collectRequired(nested, prefix + message.getName() + ".", out);
    }
  }
}
