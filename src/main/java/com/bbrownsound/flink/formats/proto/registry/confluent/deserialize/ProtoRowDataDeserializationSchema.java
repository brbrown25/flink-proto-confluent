package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.google.protobuf.DynamicMessage;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoRowDataDeserializationSchema implements DeserializationSchema<RowData> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(ProtoRowDataDeserializationSchema.class);
  private static final int LOG_EVERY_N_MESSAGES = 100;

  /** Type information for the produced RowData. */
  private final TypeInformation<RowData> resultTypeInfo;
  /** Flink row type for the deserialized data. */
  private final RowType rowType;
  /** Format and Schema Registry config. */
  private final ProtoConfluentFormatConfig formatConfig;
  private transient CachedSchemaRegistryClient client;
  private transient KafkaProtobufDeserializer<DynamicMessage> deserializer;
  private transient Map<Integer, ProtoToRowDataConverters.ProtoToRowDataConverter> converters;
  private transient AtomicLong deserializeCount;

  /**
   * Creates a deserialization schema for the given row type and config.
   *
   * @param rowType Flink row type for the deserialized data
   * @param resultTypeInfo type information for the produced RowData
   * @param config format and Schema Registry configuration
   */
  public ProtoRowDataDeserializationSchema(
      RowType rowType, TypeInformation<RowData> resultTypeInfo, ProtoConfluentFormatConfig config) {
    this.rowType = rowType;
    this.resultTypeInfo = resultTypeInfo;
    this.formatConfig = config;
  }

  @Override
  public void open(InitializationContext context) {
    if (this.client == null) {
      LOG.info(
          "[proto-confluent] ProtoRowDataDeserializationSchema.open: schemaRegistryURL={}, "
              + "topic={}, isKey={}",
          formatConfig.schemaRegistryUrl,
          formatConfig.topic,
          formatConfig.isKey);

      this.client =
          new CachedSchemaRegistryClient(
              formatConfig.schemaRegistryUrl,
              100,
              Collections.singletonList(new ProtobufSchemaProvider()),
              formatConfig.getProperties());
      this.deserializer = new KafkaProtobufDeserializer<>(client);
      this.deserializer.configure(formatConfig.getProperties(), formatConfig.isKey);
      this.converters = new ConcurrentHashMap<>();
      this.deserializeCount = new AtomicLong(0);
      LOG.debug("[proto-confluent] deserializer.open: client and deserializer initialized");
    }
  }

  @Override
  public RowData deserialize(byte[] message) throws IOException {
    if (message == null) {
      return null;
    }
    if (deserializeCount == null) {
      deserializeCount = new AtomicLong(0);
    }
    long n = deserializeCount.incrementAndGet();
    if (n <= 5 || (n % LOG_EVERY_N_MESSAGES == 0)) {
      LOG.info(
          "[proto-confluent] deserialize called #{}: messageLength={}, topic={}",
          n,
          message.length,
          formatConfig.topic);
      LOG.debug(
          "[proto-confluent] deserialize #"
              + n
              + ": messageLength="
              + message.length
              + ", topic="
              + formatConfig.topic);
    }
    try {
      DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(message));
      inputStream.readByte(); // skip magic byte
      int id = inputStream.readInt();
      if (n <= 3) {
        LOG.debug("[proto-confluent] deserialize #{}: schemaId={}", n, id);
      }
      DynamicMessage dynamicMessage = deserializer.deserialize(formatConfig.topic, message);

      ProtoToRowDataConverters.ProtoToRowDataConverter converter =
          Optional.ofNullable(converters.get(id))
              .orElseGet(
                  () -> {
                    var res =
                        ProtoToRowDataConverters.createConverter(
                            dynamicMessage.getDescriptorForType(), rowType);
                    converters.put(id, res);
                    if (n <= 3) {
                      LOG.info(
                          "[proto-confluent] deserialize #{}: created "
                              + "converter for schemaId={}, descriptor={}",
                          n,
                          id,
                          dynamicMessage.getDescriptorForType().getFullName());
                    }
                    return res;
                  });
      RowData rowData = (RowData) converter.convert(dynamicMessage);
      if (n <= 5) {
        LOG.debug("[print-source] message #" + n + " from topic: " + rowData);
      }
      return rowData;
    } catch (Exception e) {
      LOG.error(
          "[proto-confluent] deserialize failed: messageLength={}, topic={}, deserializeCount={}",
          message.length,
          formatConfig.topic,
          n,
          e);
      LOG.error(
          "[proto-confluent] deserialize FAILED #"
              + n
              + ": "
              + e.getClass().getSimpleName()
              + ": "
              + e.getMessage());
      throw new IOException("Proto-confluent deserialize failed: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return resultTypeInfo;
  }
}
