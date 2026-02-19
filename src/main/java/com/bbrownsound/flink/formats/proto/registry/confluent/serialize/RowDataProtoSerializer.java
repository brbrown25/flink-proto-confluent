package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.RowTypeToProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class RowDataProtoSerializer extends KafkaProtobufSerializer<DynamicMessage> {
  private final Map<String, RowDataToProtoConverters.RowDataToProtoConverter> converters;
  private final Map<String, Descriptors.Descriptor> schemaCache;

  RowDataProtoSerializer(SchemaRegistryClient client) {
    super(client);
    this.converters = new ConcurrentHashMap<>();
    this.schemaCache = new ConcurrentHashMap<>();
  }

  private Descriptors.Descriptor getDescriptor(String subject, RowType rowType) {
    Descriptors.Descriptor desc;

    try {
      SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(subject);
      Optional<ParsedSchema> optSchema =
          schemaRegistry.parseSchema(
              new io.confluent.kafka.schemaregistry.client.rest.entities.Schema(
                  null, schemaMetadata));
      ParsedSchema latestVersion =
          optSchema.orElseThrow(
              () ->
                  new IOException(
                      "Invalid schema "
                          + schemaMetadata.getSchema()
                          + " with refs "
                          + schemaMetadata.getReferences()
                          + " of type "
                          + schemaMetadata.getSchemaType()));
      ProtobufSchema schema = (ProtobufSchema) latestVersion;
      desc = schema.toDescriptor();
      schemaCache.put(subject, desc);
    } catch (IOException | RestClientException e) {
      desc = RowTypeToProto.fromRowType(rowType, "Row", "com.flink.proto.confluent");
    }
    if (desc == null) {
      throw new ValidationException("Could not find schema for subject: " + subject);
    }
    return desc;
  }

  /**
   * Serializes a Flink RowData to Confluent wire format (magic byte + schema id + protobuf bytes).
   *
   * @param topic the topic name for subject resolution
   * @param rowType the Flink row type
   * @param row the row data to serialize
   * @return the serialized bytes
   */
  public byte[] serializeRowData(String topic, RowType rowType, RowData row) {
    String subject = getSubjectName(topic, isKey, null, null);
    Descriptors.Descriptor desc;

    if (schemaCache.containsKey(subject)) {
      desc = schemaCache.get(subject);
    } else {
      desc = getDescriptor(subject, rowType);
    }

    Descriptors.Descriptor finalDesc = desc;
    RowDataToProtoConverters.RowDataToProtoConverter converter =
        Optional.ofNullable(this.converters.get(finalDesc.getFullName()))
            .orElseGet(
                () -> {
                  RowDataToProtoConverters.RowDataToProtoConverter newConverter =
                      RowDataToProtoConverters.createConverter(rowType, finalDesc);
                  this.converters.put(finalDesc.getFullName(), newConverter);
                  return newConverter;
                });
    return serialize(topic, (DynamicMessage) converter.convert(row));
  }
}
