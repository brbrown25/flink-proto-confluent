package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

class ProtoDecodingFormatTest {

  private static final DataType SIMPLE_TABLE =
      DataTypes.ROW(
          DataTypes.FIELD("content", DataTypes.STRING()),
          DataTypes.FIELD("date_time", DataTypes.STRING()));

  private static final DataType MISMATCHED_TABLE =
      DataTypes.ROW(
          DataTypes.FIELD("content", DataTypes.INT()),
          DataTypes.FIELD("order_id", DataTypes.STRING()));

  /** A planner context that hands out no type information; the decoder only stores it. */
  private static final DynamicTableSource.Context CONTEXT =
      new DynamicTableSource.Context() {
        @Override
        public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
          return null;
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
          return null;
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(
            DataType producedDataType) {
          return null;
        }
      };

  private static ProtoConfluentFormatConfig config(String messageClass) {
    return new ProtoConfluentFormatConfig(
        "http://localhost:8081",
        "t",
        false,
        messageClass == null ? Map.of() : Map.of("value.message-class", messageClass));
  }

  @Test
  void getChangelogMode() {
    ProtoDecodingFormat format = new ProtoDecodingFormat(config(null));
    assertEquals(ChangelogMode.insertOnly(), format.getChangelogMode());
  }

  @Test
  void createRuntimeDecoder_withoutMessageClass_acceptsAnyTable() {
    ProtoDecodingFormat format = new ProtoDecodingFormat(config(null));
    assertNotNull(format.createRuntimeDecoder(CONTEXT, MISMATCHED_TABLE));
  }

  @Test
  void createRuntimeDecoder_matchingMessageClass() {
    ProtoDecodingFormat format =
        new ProtoDecodingFormat(config(TestSimple.SimpleMessage.class.getName()));
    assertNotNull(format.createRuntimeDecoder(CONTEXT, SIMPLE_TABLE));
  }

  @Test
  void createRuntimeDecoder_mismatchedMessageClass_failsWhenPlanned() {
    ProtoDecodingFormat format =
        new ProtoDecodingFormat(
            config(TestSimple.SimpleMessage.class.getName()), getClass().getClassLoader());
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> format.createRuntimeDecoder(CONTEXT, MISMATCHED_TABLE));
    assertTrue(e.getMessage().contains("column 'content' is INT"), e.getMessage());
    assertTrue(e.getMessage().contains("column 'order_id' has no field"), e.getMessage());
  }

  @Test
  void createRuntimeDecoder_conflictingMessageName_failsWhenPlanned() {
    ProtoConfluentFormatConfig config = config(TestSimple.SimpleMessage.class.getName());
    config.messageName = "com.example.Order";
    ProtoDecodingFormat format = new ProtoDecodingFormat(config, null);
    ValidationException e =
        assertThrows(
            ValidationException.class, () -> format.createRuntimeDecoder(CONTEXT, SIMPLE_TABLE));
    assertTrue(e.getMessage().contains("'message-name' = 'com.example.Order'"), e.getMessage());
  }

  @Test
  void createRuntimeDecoder_classThePlannerCannotSee_isDeferredToOpen() {
    // The class is checked when the source opens on the TaskManager instead.
    ProtoDecodingFormat format =
        new ProtoDecodingFormat(config("com.example.OnlyInTheJobJar$Order"), null);
    assertNotNull(format.createRuntimeDecoder(CONTEXT, MISMATCHED_TABLE));
  }
}
