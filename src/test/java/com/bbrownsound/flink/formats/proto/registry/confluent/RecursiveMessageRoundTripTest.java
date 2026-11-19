package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoToRowDataConverters;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.SourceMessageValidator;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.RowDataToProtoConverters;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToTableDDL;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RecursionStrategy;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.junit.jupiter.api.Test;

/**
 * Derived DDL for a recursive message (issue #216) round-trips: the column list {@link
 * ProtoToTableDDL} derives with each {@link RecursionStrategy}, parsed back as Flink SQL, passes the
 * {@code message-class} check, reads a message and writes it back. {@code bytes} and {@code json}
 * keep every level; {@code row:<n>} keeps {@code n} levels below the table's message.
 */
class RecursiveMessageRoundTripTest {

  private static final Descriptor TREE =
      (Descriptor)
          new ProtobufSchema(
                  "syntax = \"proto3\"; package rec; message Tree { string name = 1; Tree child = 2;"
                      + " repeated Tree children = 3; map<string, Tree> by_name = 4;"
                      + " oneof kind { string label = 5; Tree alt = 6; } }")
              .toDescriptor();

  private static final FieldDescriptor CHILD = TREE.findFieldByName("child");
  private static final FieldDescriptor CHILDREN = TREE.findFieldByName("children");
  private static final FieldDescriptor BY_NAME = TREE.findFieldByName("by_name");
  private static final FieldDescriptor ALT = TREE.findFieldByName("alt");

  private static DynamicMessage.Builder tree(String name) {
    return DynamicMessage.newBuilder(TREE).setField(TREE.findFieldByName("name"), name);
  }

  private static DynamicMessage.Builder tree(String name, Message child) {
    return tree(name).setField(CHILD, child);
  }

  private static Message entry(String key, Message value) {
    Descriptor entry = BY_NAME.getMessageType();
    return DynamicMessage.newBuilder(entry)
        .setField(entry.findFieldByName("key"), key)
        .setField(entry.findFieldByName("value"), value)
        .build();
  }

  /** Four levels through {@code child}, and a second level through every other recursive field. */
  private static Message deepTree() {
    return tree("a", tree("b", tree("c", tree("d").build()).build()).build())
        .addRepeatedField(CHILDREN, tree("x", tree("y").build()).build())
        .addRepeatedField(BY_NAME, entry("k", tree("m", tree("n").build()).build()))
        .setField(ALT, tree("p").setField(ALT, tree("r", tree("s").build()).build()).build())
        .build();
  }

  private static Message roundTrip(RecursionStrategy recursion) throws Exception {
    String columns = ProtoToTableDDL.tableSchemaFromDescriptor(TREE, false, null, recursion);
    RowType type =
        (RowType)
            LogicalTypeParser.parse(
                "ROW<" + columns + ">", RecursiveMessageRoundTripTest.class.getClassLoader());
    SourceMessageValidator.validateRowType(TREE, type, "rec.Tree");

    Message source = DynamicMessage.parseFrom(TREE, deepTree().toByteArray());
    RowData row = (RowData) ProtoToRowDataConverters.createConverter(TREE, type).convert(source);
    Message written = (Message) RowDataToProtoConverters.createConverter(type, TREE).convert(row);
    return DynamicMessage.parseFrom(TREE, written.toByteArray());
  }

  @Test
  void bytesKeepsEveryLevel() throws Exception {
    assertEquals(deepTree(), roundTrip(RecursionStrategy.BYTES));
  }

  @Test
  void jsonKeepsEveryLevel() throws Exception {
    assertEquals(deepTree(), roundTrip(RecursionStrategy.JSON));
  }

  @Test
  void rowOneKeepsOneLevelBelowTheTableMessage() throws Exception {
    Message expected =
        tree("a", tree("b").build())
            .addRepeatedField(CHILDREN, tree("x").build())
            .addRepeatedField(BY_NAME, entry("k", tree("m").build()))
            // The oneof case at the last level is kept serialized, so it keeps every level.
            .setField(ALT, tree("p").setField(ALT, tree("r", tree("s").build()).build()).build())
            .build();
    assertEquals(expected, roundTrip(RecursionStrategy.row(1)));
  }

  @Test
  void rowTwoKeepsTwoLevelsBelowTheTableMessage() throws Exception {
    Message expected =
        tree("a", tree("b", tree("c").build()).build())
            .addRepeatedField(CHILDREN, tree("x", tree("y").build()).build())
            .addRepeatedField(BY_NAME, entry("k", tree("m", tree("n").build()).build()))
            .setField(ALT, tree("p").setField(ALT, tree("r").build()).build())
            .build();
    assertEquals(expected, roundTrip(RecursionStrategy.row(2)));
  }
}
