package com.bbrownsound.flink.formats.proto.registry.confluent.docs;

// --8<-- [start:example]
import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoRowDataDeserializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.ProtoRowDataSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/** Reads and writes Confluent-framed Protobuf records with the DataStream API. */
public final class DataStreamExample {

  /** The row type of each record: one field per Protobuf field, matched by name. */
  public static final RowType ORDER =
      (RowType)
          DataTypes.ROW(
                  DataTypes.FIELD("order_id", DataTypes.STRING()),
                  DataTypes.FIELD("quantity", DataTypes.INT()),
                  DataTypes.FIELD("amount", DataTypes.DECIMAL(10, 2)))
              .getLogicalType();

  private DataStreamExample() {}

  /**
   * The format options, as a table's {@code value.proto-confluent.*} options would set them
   * (without the prefix). Any option of the format can be set here.
   */
  public static ProtoConfluentFormatConfig formatConfig(
      String schemaRegistryUrl, String topic, boolean autoRegisterSchemas) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, schemaRegistryUrl);
    options.set(ProtoConfluentFormatOptions.TOPIC, topic);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
    return new ProtoConfluentFormatConfig(options);
  }

  /** A Kafka source that decodes each record value with the writer schema named in its header. */
  public static KafkaSource<RowData> source(
      String bootstrapServers, String schemaRegistryUrl, String topic) {
    ProtoRowDataDeserializationSchema valueDeserializer =
        new ProtoRowDataDeserializationSchema(
            ORDER, InternalTypeInfo.of(ORDER), formatConfig(schemaRegistryUrl, topic, false));
    return KafkaSource.<RowData>builder()
        .setBootstrapServers(bootstrapServers)
        .setTopics(topic)
        .setGroupId("orders-reader")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(valueDeserializer)
        .build();
  }

  /**
   * A Kafka sink that writes each row with the latest schema of the topic's subject, or, while the
   * subject does not exist, with a schema derived from {@link #ORDER} that it registers.
   */
  public static KafkaSink<RowData> sink(
      String bootstrapServers, String schemaRegistryUrl, String topic) {
    ProtoRowDataSerializationSchema valueSerializer =
        new ProtoRowDataSerializationSchema(ORDER, formatConfig(schemaRegistryUrl, topic, true));
    return KafkaSink.<RowData>builder()
        .setBootstrapServers(bootstrapServers)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<RowData>builder()
                .setTopic(topic)
                .setValueSerializationSchema(valueSerializer)
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  /** Copies one topic to another: a pipeline that uses both. */
  public static void copy(
      StreamExecutionEnvironment env,
      String bootstrapServers,
      String schemaRegistryUrl,
      String inputTopic,
      String outputTopic) {
    DataStream<RowData> orders =
        env.fromSource(
            source(bootstrapServers, schemaRegistryUrl, inputTopic),
            WatermarkStrategy.noWatermarks(),
            "orders");
    orders.sinkTo(sink(bootstrapServers, schemaRegistryUrl, outputTopic));
  }
}
// --8<-- [end:example]
