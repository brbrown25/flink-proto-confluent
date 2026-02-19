package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;

class ProtoConfluentFormatConfigTest {

  @Test
  void constructorFromParams() {
    Map<String, String> props = Map.of("k", "v");
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://sr:8081", "my-topic", true, props);
    assertEquals("http://sr:8081", config.schemaRegistryUrl);
    assertEquals("my-topic", config.topic);
    assertTrue(config.isKey);
    assertEquals(props, config.getProperties());
  }

  @Test
  void constructorFromReadableConfig_defaults() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "t");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("http://localhost:8081", config.schemaRegistryUrl);
    assertEquals("t", config.topic);
    assertFalse(config.isKey);
    assertNotNull(config.getProperties());
    assertEquals("http://localhost:8081", config.getProperties().get("schema.registry.url"));
    assertEquals("false", config.getProperties().get("auto.register.schemas"));
    assertEquals("true", config.getProperties().get("normalize.schemas"));
    assertEquals("-1", config.getProperties().get("use.schema.id"));
    assertEquals("true", config.getProperties().get("skip.known.types"));
  }

  @Test
  void constructorFromReadableConfig_withOptionalOptions() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.IS_KEY, true);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, true);
    options.set(ProtoConfluentFormatOptions.NORMALIZE_SCHEMAS, false);
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertTrue(config.isKey);
    assertEquals("true", config.getProperties().get("auto.register.schemas"));
    assertEquals("false", config.getProperties().get("normalize.schemas"));
  }
}
