package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bbrownsound.flink.formats.proto.registry.confluent.docs.DataStreamExample;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Runs the DataStream API example that docs/content/how-to/datastream-api.md includes, against
 * Kafka and Schema Registry: rows written by its sink, copied by its source and sink to a second
 * topic, read back by its source (GitHub issue #178).
 */
@DisplayName("The DataStream API example reads and writes proto-confluent records")
class DataStreamExampleIntegrationTest extends ProtoConfluentTestBase {

  private static MiniClusterWithClientResource flinkCluster;
  private static String bootstrap;

  @BeforeAll
  static void setUpCluster() throws Exception {
    bootstrap = kafka.getHost() + ":" + kafka.getMappedPort(9093);
    flinkCluster =
        new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(4)
                .build());
    flinkCluster.before();
  }

  @AfterAll
  static void tearDownCluster() {
    if (flinkCluster != null) {
      flinkCluster.after();
    }
  }

  @Test
  void writesCopiesAndReadsOrders() throws Exception {
    String registry = getSchemaRegistryUrl();
    String input = "datastream-orders";
    String output = "datastream-orders-copy";
    // The reader starts while the copy job may not have written yet, so the topics exist first.
    try (AdminClient admin =
        AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
      admin
          .createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(output, 1, (short) 1)))
          .all()
          .get();
    }
    List<RowData> orders =
        List.of(order("o-1", 2, new BigDecimal("19.99")), order("o-2", 5, new BigDecimal("3.50")));

    StreamExecutionEnvironment writer = StreamExecutionEnvironment.getExecutionEnvironment();
    writer.setParallelism(1);
    writer
        .fromData(InternalTypeInfo.of(DataStreamExample.ORDER), orders.toArray(new RowData[0]))
        .sinkTo(DataStreamExample.sink(bootstrap, registry, input));
    writer.execute("write orders");

    StreamExecutionEnvironment copier = StreamExecutionEnvironment.getExecutionEnvironment();
    copier.setParallelism(1);
    DataStreamExample.copy(copier, bootstrap, registry, input, output);
    JobClient copyJob = copier.executeAsync("copy orders");
    try {
      StreamExecutionEnvironment reader = StreamExecutionEnvironment.getExecutionEnvironment();
      reader.setParallelism(1);
      List<String> read = new ArrayList<>();
      try (CloseableIterator<RowData> rows =
          reader
              .fromSource(
                  DataStreamExample.source(bootstrap, registry, output),
                  WatermarkStrategy.noWatermarks(),
                  "copied orders")
              .executeAndCollect("read copied orders")) {
        while (read.size() < orders.size() && rows.hasNext()) {
          read.add(describe(rows.next()));
        }
      }
      List<String> expected = new ArrayList<>();
      for (RowData order : orders) {
        expected.add(describe(order));
      }
      assertEquals(expected, read);
    } finally {
      copyJob.cancel().get();
    }
  }

  /** The row's fields; the collected rows come back as BinaryRowData, without a readable toString. */
  private static String describe(RowData row) {
    return row.getString(0) + "," + row.getInt(1) + "," + row.getDecimal(2, 10, 2);
  }

  private static RowData order(String id, int quantity, BigDecimal amount) {
    return GenericRowData.of(
        StringData.fromString(id), quantity, DecimalData.fromBigDecimal(amount, 10, 2));
  }
}
