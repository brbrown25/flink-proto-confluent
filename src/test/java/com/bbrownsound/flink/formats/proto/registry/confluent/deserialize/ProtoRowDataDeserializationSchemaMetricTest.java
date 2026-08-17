package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;

/**
 * Covers GitHub issue #69: asserts the numDeserializeErrors metric counter actually increments on
 * failed records. Existing tests called {@code schema.open(null)}, which skips metric wiring
 * entirely ({@code ProtoRowDataDeserializationSchema.open} only creates the counter when the
 * {@code InitializationContext} is non-null), so the counter was never previously exercised.
 */
class ProtoRowDataDeserializationSchemaMetricTest {

  private static DeserializationSchema.InitializationContext contextFor(MetricListener listener) {
    return new DeserializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return listener.getMetricGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(
            ProtoRowDataDeserializationSchemaMetricTest.class.getClassLoader());
      }
    };
  }

  @Test
  void deserialize_incrementsNumDeserializeErrorsMetricOnFailure() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    Map<String, String> props = Map.of("schema.registry.url", "http://localhost:8081");
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://localhost:8081", "test-topic", false, props);
    config.onDeserializeError = "skip";

    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    schema.open(contextFor(listener));

    byte[] invalid = new byte[] {0, 1, 2, 3, 4};

    // The counter is registered eagerly in open(), starting at zero.
    assertEquals(0L, listener.getCounter("numDeserializeErrors").get().getCount());

    RowData first = schema.deserialize(invalid);
    assertEquals(null, first);
    var counterAfterOne = listener.getCounter("numDeserializeErrors");
    assertTrue(counterAfterOne.isPresent(), "numDeserializeErrors counter should be registered");
    assertEquals(1L, counterAfterOne.get().getCount());

    schema.deserialize(invalid);
    assertEquals(2L, listener.getCounter("numDeserializeErrors").get().getCount());
  }

  @Test
  void deserialize_doesNotIncrementMetricOnSuccessfulRecordCountOnlyOnFailure() throws IOException {
    RowType rowType =
        (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
    Map<String, String> props = Map.of("schema.registry.url", "http://localhost:8081");
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://localhost:8081", "test-topic", false, props);
    config.onDeserializeError = "skip";

    MetricListener listener = new MetricListener();
    var schema = new ProtoRowDataDeserializationSchema(rowType, null, config);
    schema.open(contextFor(listener));

    // null input is a no-op short-circuit, not a deserialize failure: must not touch the metric.
    assertEquals(null, schema.deserialize(null));
    assertEquals(0L, listener.getCounter("numDeserializeErrors").get().getCount());
  }
}
