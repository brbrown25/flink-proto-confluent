package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import java.util.Collections;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoRowDataSerializationSchema implements SerializationSchema<RowData> {
  private static final long serialVersionUID = 1L;
  /** Flink row type for the serialized data. */
  private final RowType rowType;

  /** Format and Schema Registry config. */
  private final ProtoConfluentFormatConfig formatConfig;
  private transient CachedSchemaRegistryClient client;
  private transient RowDataProtoSerializer serializer;

  /**
   * Creates a serialization schema for the given row type and config.
   *
   * @param rowType Flink row type for the data to serialize
   * @param config format and Schema Registry configuration
   */
  public ProtoRowDataSerializationSchema(RowType rowType, ProtoConfluentFormatConfig config) {
    this.rowType = rowType;
    this.formatConfig = config;
  }

  @Override
  public void open(InitializationContext context) {
    if (this.client == null) {
      this.client =
          new CachedSchemaRegistryClient(
              formatConfig.schemaRegistryUrl,
              100,
              Collections.singletonList(new ProtobufSchemaProvider()),
              formatConfig.getProperties());
      this.serializer = new RowDataProtoSerializer(client);
      this.serializer.configure(formatConfig.getProperties(), formatConfig.isKey);
    }
  }

  @Override
  public byte[] serialize(RowData element) {
    return this.serializer.serializeRowData(formatConfig.topic, rowType, element);
  }
}
