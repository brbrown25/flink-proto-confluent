package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.ReadableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.RegistryProtoFormatFactory;

/**
 * Derived from https://github.com/amstee/flink-proto-confluent (Apache-2.0). See NOTICE in project
 * root.
 */
public class ProtoConfluentFormatConfig implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoConfluentFormatConfig.class);

  /** Schema Registry base URL. */
  public String schemaRegistryUrl;

  /** Kafka topic name for schema subject resolution. */
  public String topic;

  /** Whether this format is used for the key (vs value). */
  public Boolean isKey;

  /** Optional properties passed to Schema Registry client. */
  private Map<String, String> properties;

  /**
   * Constructs config from explicit parameters.
   *
   * @param schemaRegistryURL Schema Registry base URL
   * @param topic Kafka topic name for schema subject
   * @param isKey whether this format is for the key
   * @param properties optional properties for the Schema Registry client
   */
  public ProtoConfluentFormatConfig(
      String schemaRegistryURL, String topic, Boolean isKey, Map<String, String> properties) {
    this.schemaRegistryUrl = schemaRegistryURL;
    this.topic = topic;
    this.isKey = isKey;
    this.properties = properties == null ? new HashMap<>() : new HashMap<>(properties);
  }

  /**
   * Constructs config from Flink format options.
   *
   * @param formatOptions the format options from the table environment
   */
  public ProtoConfluentFormatConfig(ReadableConfig formatOptions) {
    Boolean autoRegister = formatOptions.get(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS);
    Boolean normalize = formatOptions.get(ProtoConfluentFormatOptions.NORMALIZE_SCHEMAS);
    Integer schemaId = formatOptions.get(ProtoConfluentFormatOptions.USE_SCHEMA_ID);
    Boolean skipKnown = formatOptions.get(ProtoConfluentFormatOptions.SKIP_KNOWN_TYPES);
    schemaRegistryUrl = formatOptions.get(ProtoConfluentFormatOptions.URL);
    topic = formatOptions.get(ProtoConfluentFormatOptions.TOPIC);
    isKey = formatOptions.get(ProtoConfluentFormatOptions.IS_KEY);

    properties = RegistryProtoFormatFactory.buildOptionalPropertiesMap(formatOptions);
    if (properties == null) {
      properties = new HashMap<>();
    }
  properties.put("schema.registry.url", schemaRegistryUrl);
  properties.put("auto.register.schemas", autoRegister.toString());
  properties.put("normalize.schemas", normalize.toString());
  properties.put("use.schema.id", schemaId.toString());
  properties.put("skip.known.types", skipKnown.toString());
    LOG.debug(
        "[proto-confluent] FormatConfig from options: schemaRegistryURL={}, topic={}, "
            + "isKey={}, propertiesKeys={}",
        schemaRegistryUrl,
        topic,
        isKey,
        properties.keySet());
  }

  /**
   * Returns a copy of the serializer/deserializer properties map.
   *
   * @return copy of the properties map, never null
   */
  public Map<String, String> getProperties() {
    return properties == null ? Collections.emptyMap() : new HashMap<>(properties);
  }
}
