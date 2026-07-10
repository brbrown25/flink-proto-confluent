package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.RowTypeToProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class RowDataProtoSerializer extends KafkaProtobufSerializer<DynamicMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(RowDataProtoSerializer.class);

  /**
   * Config key for an explicit Protobuf message class used when this serializer encodes the value
   * (i.e. {@code isKey == false}), e.g. for schema registration.
   */
  public static final String VALUE_MESSAGE_CLASS_CONFIG = "value.message-class";

  /**
   * Config key for an explicit Protobuf message class used when this serializer encodes the key
   * (i.e. {@code isKey == true}). Companion to {@link #VALUE_MESSAGE_CLASS_CONFIG} so a Kafka sink
   * can pin an explicit named entity for its key instead of a dynamic schema.
   */
  public static final String KEY_MESSAGE_CLASS_CONFIG = "key.message-class";

  private final Map<String, RowDataToProtoConverters.RowDataToProtoConverter> converters;
  private final Map<String, Descriptors.Descriptor> schemaCache;

  /**
   * When set via {@value #KEY_MESSAGE_CLASS_CONFIG} (key) or {@value #VALUE_MESSAGE_CLASS_CONFIG}
   * (value), used for serialization and registration in place of a dynamic schema.
   */
  private volatile Descriptors.Descriptor messageClassDescriptor;

  RowDataProtoSerializer(SchemaRegistryClient client) {
    super(client);
    this.converters = new ConcurrentHashMap<>();
    this.schemaCache = new ConcurrentHashMap<>();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    super.configure(configs, isKey);
    // A single format instance serves either the key or the value, so select the matching config
    // key. This keeps key and value message classes independent for a table that pins both.
    String configKey = isKey ? KEY_MESSAGE_CLASS_CONFIG : VALUE_MESSAGE_CLASS_CONFIG;
    Object messageClassObj = configs.get(configKey);
    if (messageClassObj instanceof String) {
      String className = (String) messageClassObj;
      if (!className.isEmpty()) {
        try {
          this.messageClassDescriptor = descriptorFromMessageClass(className);
          LOG.info(
              "[proto-confluent] Using message class for {} serialization: {} -> {}",
              isKey ? "key" : "value",
              className,
              messageClassDescriptor.getFullName());
        } catch (Exception e) {
          throw new ValidationException(
              "Failed to resolve descriptor for " + configKey + "=" + className, e);
        }
      }
    }
  }

  private static Descriptors.Descriptor descriptorFromMessageClass(String className)
      throws ReflectiveOperationException {
    Class<?> clazz = Class.forName(className);
    Method getDefault = clazz.getMethod("getDefaultInstance");
    Message defaultInstance = (Message) getDefault.invoke(null);
    return defaultInstance.getDescriptorForType();
  }

  private Descriptors.Descriptor getDescriptor(String subject, RowType rowType) {
    if (messageClassDescriptor != null) {
      return messageClassDescriptor;
    }
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
