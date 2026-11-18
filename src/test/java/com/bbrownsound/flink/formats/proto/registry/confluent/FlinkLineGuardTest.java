package com.bbrownsound.flink.formats.proto.registry.confluent;

import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.assertLoadedFrom;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.assertMatchesCiMatrix;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.assertRuntimeVersion;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.requestedFlinkVersion;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.factories.Factory;
import org.junit.jupiter.api.Test;

/** Fails the unit test run if it is not running on the Flink version it was asked to test. */
class FlinkLineGuardTest {

  @Test
  void unitTestsRunOnTheRequestedFlinkVersion() {
    final String requested = requestedFlinkVersion();
    assertMatchesCiMatrix(requested);
    assertRuntimeVersion(requested);
    assertLoadedFrom(ExecutionConfig.class, "flink-core", requested);
    assertLoadedFrom(Factory.class, "flink-table-common", requested);
  }
}
