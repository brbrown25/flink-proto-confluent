package com.bbrownsound.flink.formats.proto.registry.confluent;

import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.assertConnectorTargetsLine;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.assertLoadedFrom;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.assertMatchesCiMatrix;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.assertRuntimeVersion;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.requestedFlinkVersion;
import static com.bbrownsound.flink.formats.proto.registry.confluent.FlinkLineAssertions.requestedKafkaConnectorVersion;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.Factory;
import org.junit.jupiter.api.Test;

/**
 * Fails the integration test run if the MiniCluster, Table API or Kafka connector it uses are not
 * from the Flink line it was asked to test.
 */
class FlinkLineGuardIntegrationTest {

  @Test
  void integrationTestsRunOnTheRequestedFlinkLine() {
    final String requested = requestedFlinkVersion();
    assertMatchesCiMatrix(requested);
    assertRuntimeVersion(requested);
    assertLoadedFrom(ExecutionConfig.class, "flink-core", requested);
    assertLoadedFrom(Factory.class, "flink-table-common", requested);
    // DiscardingSink lives in flink-streaming-java on both lines; StreamExecutionEnvironment moved
    // into flink-runtime in 2.x, so it cannot identify the streaming JAR.
    assertLoadedFrom(DiscardingSink.class, "flink-streaming-java", requested);
    assertLoadedFrom(StreamTableEnvironment.class, "flink-table-api-java-bridge", requested);

    final String connector = requestedKafkaConnectorVersion();
    assertConnectorTargetsLine(connector, requested);
    assertLoadedFrom(KafkaSource.class, "flink-connector-kafka", connector);
  }
}
