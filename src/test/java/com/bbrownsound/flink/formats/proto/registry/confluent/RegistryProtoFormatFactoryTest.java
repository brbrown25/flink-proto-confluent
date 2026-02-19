package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class RegistryProtoFormatFactoryTest {

  private final RegistryProtoFormatFactory factory = new RegistryProtoFormatFactory();

  @Test
  void factoryIdentifier() {
    assertEquals("proto-confluent", factory.factoryIdentifier());
  }

  @Test
  void requiredOptions() {
    var required = factory.requiredOptions();
    assertEquals(2, required.size());
    assertTrue(required.stream().anyMatch(o -> o.key().equals("url")));
    assertTrue(required.stream().anyMatch(o -> o.key().equals("topic")));
  }

  @Test
  void optionalOptions() {
    var optional = factory.optionalOptions();
    assertTrue(optional.size() >= 4);
    assertTrue(optional.stream().anyMatch(o -> o.key().equals("is_key")));
    assertTrue(optional.stream().anyMatch(o -> o.key().equals("properties")));
  }

  @Test
  void forwardOptions() {
    var forward = factory.forwardOptions();
    assertTrue(forward.stream().anyMatch(o -> o.key().equals("url")));
    assertTrue(forward.stream().anyMatch(o -> o.key().equals("topic")));
  }

  @Test
  void buildOptionalPropertiesMap_empty() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "t");
    Map<String, String> props = RegistryProtoFormatFactory.buildOptionalPropertiesMap(options);
    assertNull(props);
  }

  @Test
  void buildOptionalPropertiesMap_withProperties() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "t");
    options.set(ProtoConfluentFormatOptions.PROPERTIES, Map.of("custom.key", "custom.value"));
    Map<String, String> props = RegistryProtoFormatFactory.buildOptionalPropertiesMap(options);
    assertNotNull(props);
    assertEquals("custom.value", props.get("custom.key"));
  }

  @Test
  void buildOptionalPropertiesMap_withSslAndAuth() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION, "/path/to/keystore");
    options.set(ProtoConfluentFormatOptions.SSL_KEYSTORE_PASSWORD, "secret");
    options.set(ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
    options.set(ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO, "user:pass");
    Map<String, String> props = RegistryProtoFormatFactory.buildOptionalPropertiesMap(options);
    assertNotNull(props);
    assertEquals("/path/to/keystore", props.get("schema.registry.ssl.keystore.location"));
    assertEquals("secret", props.get("schema.registry.ssl.keystore.password"));
    assertEquals("USER_INFO", props.get("basic.auth.credentials.source"));
    assertEquals("user:pass", props.get("basic.auth.user.info"));
  }

  @Test
  void buildOptionalPropertiesMap_withTruststoreAndBearerAuth() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_LOCATION, "/path/to/truststore");
    options.set(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD, "trustsecret");
    options.set(ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE, "STATIC");
    options.set(ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN, "token123");
    Map<String, String> props = RegistryProtoFormatFactory.buildOptionalPropertiesMap(options);
    assertNotNull(props);
    assertEquals("/path/to/truststore", props.get("schema.registry.ssl.truststore.location"));
    assertEquals("trustsecret", props.get("schema.registry.ssl.truststore.password"));
    assertEquals("STATIC", props.get("bearer.auth.credentials.source"));
    assertEquals("token123", props.get("bearer.auth.token"));
  }

  @Test
  void createDecodingFormat_returnsProtoDecodingFormat() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "test-topic");
    DecodingFormat<org.apache.flink.api.common.serialization.DeserializationSchema<RowData>>
        format = factory.createDecodingFormat(null, options);
    assertNotNull(format);
    assertTrue(format instanceof ProtoDecodingFormat);
  }

  @Test
  void createEncodingFormat_returnsProtoEncodingFormat() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://localhost:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "test-topic");
    EncodingFormat<org.apache.flink.api.common.serialization.SerializationSchema<RowData>> format =
        factory.createEncodingFormat(null, options);
    assertNotNull(format);
    assertTrue(format instanceof ProtoEncodingFormat);
  }
}
