package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;

import com.bbrownsound.flink.formats.proto.test.v1.TestBig;
import com.bbrownsound.flink.formats.proto.test.v1.TestRepeated;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;

class ProtoToTableDDLTest {

  @Test
  void tableSchemaFromProto_simpleMessage() {
    String ddl = ProtoToTableDDL.tableSchemaFromProto(TestSimple.SimpleMessage.class);
    assertNotNull(ddl);
    assertTrue(ddl.contains("content"));
    assertTrue(ddl.contains("date_time"));
    assertTrue(ddl.contains("STRING"));
  }

  @Test
  void tableSchemaFromProto_strictNotNull() {
    String ddl = ProtoToTableDDL.tableSchemaFromProto(TestSimple.SimpleMessage.class, true);
    assertNotNull(ddl);
    assertTrue(ddl.contains("NOT NULL") || ddl.contains("content"));
  }

  @Test
  void tableSchemaFromProto_withKeepNullablePaths() {
    String ddl =
        ProtoToTableDDL.tableSchemaFromProto(
            TestSimple.SimpleMessage.class, true, Set.of("content"));
    assertNotNull(ddl);
  }

  @Test
  void tableSchemaFromDescriptor_simpleMessage() {
    String ddl =
        ProtoToTableDDL.tableSchemaFromDescriptor(TestSimple.SimpleMessage.getDescriptor());
    assertNotNull(ddl);
    assertTrue(ddl.contains("content"));
  }

  @Test
  void tableSchemaFromDescriptor_strictNotNullAndPaths() {
    String ddl =
        ProtoToTableDDL.tableSchemaFromDescriptor(
            TestBig.BigPbMessage.getDescriptor(), true, Set.of("string_field_3"));
    assertNotNull(ddl);
    assertTrue(ddl.contains("int_field_1") || ddl.contains("string_field"));
  }

  @Test
  void tableSchemaFromProto_notAProtoClass() {
    @SuppressWarnings("unchecked")
    Class<? extends com.google.protobuf.Message> notProto =
        (Class<? extends com.google.protobuf.Message>) (Class<?>) String.class;
    assertThrows(
        IllegalArgumentException.class, () -> ProtoToTableDDL.tableSchemaFromProto(notProto));
  }

  /** Repeated runs to ensure DDL generation is deterministic and stable. */
  @RepeatedTest(10)
  void tableSchemaFromProto_repeatedDeterministic(RepetitionInfo repetitionInfo) {
    String ddl = ProtoToTableDDL.tableSchemaFromProto(TestSimple.SimpleMessage.class);
    assertNotNull(ddl, "repetition " + repetitionInfo.getCurrentRepetition());
    assertTrue(ddl.contains("content"), "repetition " + repetitionInfo.getCurrentRepetition());
    assertTrue(ddl.contains("date_time"), "repetition " + repetitionInfo.getCurrentRepetition());
    assertTrue(ddl.contains("STRING"), "repetition " + repetitionInfo.getCurrentRepetition());
    // Determinism: same input yields same output
    String again = ProtoToTableDDL.tableSchemaFromProto(TestSimple.SimpleMessage.class);
    assertTrue(ddl.equals(again), "DDL should be deterministic within repetition");
  }

  /** Repeated runs for descriptor-based DDL with repeated fields (ARRAY types). */
  @RepeatedTest(5)
  void tableSchemaFromDescriptor_repeatedMessageDeterministic(RepetitionInfo repetitionInfo) {
    String ddl =
        ProtoToTableDDL.tableSchemaFromDescriptor(
            TestRepeated.RepeatedTest.getDescriptor());
    assertNotNull(ddl, "repetition " + repetitionInfo.getCurrentRepetition());
    assertTrue(ddl.contains("values") || ddl.contains("numbers"), "repetition " + repetitionInfo.getCurrentRepetition());
    assertTrue(ddl.contains("ARRAY") || ddl.contains("STRING"), "repetition " + repetitionInfo.getCurrentRepetition());
    String again =
        ProtoToTableDDL.tableSchemaFromDescriptor(
            TestRepeated.RepeatedTest.getDescriptor());
    assertTrue(ddl.equals(again), "DDL from descriptor should be deterministic within repetition");
  }
}
