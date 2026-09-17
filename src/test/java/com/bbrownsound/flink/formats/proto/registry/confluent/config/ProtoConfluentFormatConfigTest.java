package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

  @Test
  void messageClass_routedToValueProperty_whenNotKey() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.IS_KEY, false);
    options.set(ProtoConfluentFormatOptions.MESSAGE_CLASS, "com.example.Foo$Bar");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("com.example.Foo$Bar", config.getProperties().get("value.message-class"));
    assertNull(config.getProperties().get("key.message-class"));
  }

  @Test
  void messageClass_routedToKeyProperty_whenKey() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.IS_KEY, true);
    options.set(ProtoConfluentFormatOptions.MESSAGE_CLASS, "com.example.Foo$Bar");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("com.example.Foo$Bar", config.getProperties().get("key.message-class"));
    assertNull(config.getProperties().get("value.message-class"));
  }

  @Test
  void messageClass_absentByDefault() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertNull(config.getProperties().get("value.message-class"));
    assertNull(config.getProperties().get("key.message-class"));
  }

  @Test
  void messageClass_typedOptionWinsOverTunneledProperty() {
    // Both a raw tunneled 'value.message-class' (via properties) and the typed MESSAGE_CLASS
    // option are set. The typed option must win — locks the precedence contract so a future
    // reorder of the puts in ProtoConfluentFormatConfig can't flip it silently.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.IS_KEY, false);
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of("value.message-class", "com.example.Tunneled$Old"));
    options.set(ProtoConfluentFormatOptions.MESSAGE_CLASS, "com.example.Typed$New");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("com.example.Typed$New", config.getProperties().get("value.message-class"));
  }

  @Test
  void messageClass_tunneledPropertyPreserved_whenTypedOptionUnset() {
    // The raw 'value.message-class' tunnel is the path existing consumers rely on; it must survive
    // untouched when the typed MESSAGE_CLASS option is not set.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.IS_KEY, false);
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of("value.message-class", "com.example.Tunneled$Old"));
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("com.example.Tunneled$Old", config.getProperties().get("value.message-class"));
  }

  @Test
  void messageClass_emptyValueIgnored() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.MESSAGE_CLASS, "");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertNull(config.getProperties().get("value.message-class"));
    assertNull(config.getProperties().get("key.message-class"));
  }

  @Test
  void auth_typedBasicAuthOptionsWinOverTunneledProperties() {
    // Covers issue #71: the tunneled 'properties' map is applied first and typed Flink options
    // overwrite it, so a typed 'basic-auth.user-info' must beat a tunneled
    // 'basic.auth.user.info'. Locks the documented "Flink options have higher precedence"
    // contract of the PROPERTIES option.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of(
            "basic.auth.user.info", "tunneled-user:tunneled-pass",
            "basic.auth.credentials.source", "TUNNELED_SOURCE"));
    options.set(ProtoConfluentFormatOptions.BASIC_AUTH_USER_INFO, "typed-user:typed-pass");
    options.set(ProtoConfluentFormatOptions.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("typed-user:typed-pass", config.getProperties().get("basic.auth.user.info"));
    assertEquals("USER_INFO", config.getProperties().get("basic.auth.credentials.source"));
  }

  @Test
  void auth_typedBearerAndSslOptionsWinOverTunneledProperties() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of(
            "bearer.auth.token", "tunneled-token",
            "bearer.auth.credentials.source", "TUNNELED",
            "schema.registry.ssl.keystore.location", "/tunneled/keystore.jks",
            "schema.registry.ssl.truststore.password", "tunneled-trust-pass"));
    options.set(ProtoConfluentFormatOptions.BEARER_AUTH_TOKEN, "typed-token");
    options.set(ProtoConfluentFormatOptions.BEARER_AUTH_CREDENTIALS_SOURCE, "STATIC_TOKEN");
    options.set(ProtoConfluentFormatOptions.SSL_KEYSTORE_LOCATION, "/typed/keystore.jks");
    options.set(ProtoConfluentFormatOptions.SSL_TRUSTSTORE_PASSWORD, "typed-trust-pass");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    Map<String, String> props = config.getProperties();
    assertEquals("typed-token", props.get("bearer.auth.token"));
    assertEquals("STATIC_TOKEN", props.get("bearer.auth.credentials.source"));
    assertEquals("/typed/keystore.jks", props.get("schema.registry.ssl.keystore.location"));
    assertEquals("typed-trust-pass", props.get("schema.registry.ssl.truststore.password"));
  }

  @Test
  void auth_tunneledPropertiesPreserved_whenTypedOptionsUnset() {
    // The other half of the precedence contract: a tunneled auth key must survive untouched when
    // its typed counterpart is not set, so existing tunnel-only deployments keep working.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of(
            "basic.auth.user.info", "tunneled-user:tunneled-pass",
            "bearer.auth.token", "tunneled-token"));
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals(
        "tunneled-user:tunneled-pass", config.getProperties().get("basic.auth.user.info"));
    assertEquals("tunneled-token", config.getProperties().get("bearer.auth.token"));
  }

  @Test
  void schemaRegistryUrl_typedOptionWinsOverTunneledProperty() {
    // 'schema.registry.url' is derived from the typed URL option and written after the tunnel,
    // so a tunneled value can never point the client at a different registry.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://typed-sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of(
            "schema.registry.url", "http://tunneled-sr:9081",
            "auto.register.schemas", "true",
            "use.schema.id", "42"));
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    Map<String, String> props = config.getProperties();
    assertEquals("http://typed-sr:8081", props.get("schema.registry.url"));
    assertEquals("false", props.get("auto.register.schemas"));
    assertEquals("-1", props.get("use.schema.id"));
  }

  @Test
  void onDeserializeError_defaultsToFail() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("fail", config.onDeserializeError);
  }

  @Test
  void onDeserializeError_invalidValueIsNotRejected_andIsCarriedVerbatim() {
    // Documented behavior (issue #71): the option is NOT validated. An unrecognized value is
    // accepted at config time and takes the 'skip' path at runtime, since only "fail" is fatal.
    // See ProtoRowDataDeserializationSchemaNegativeConfigTest for the runtime assertion.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, "bogus");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("bogus", config.onDeserializeError);
  }

  @Test
  void onDeserializeError_failIsCaseInsensitive() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, "FAIL");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("FAIL", config.onDeserializeError);
  }

  @Test
  void deadLetter_topicWithoutBootstrapServers_isAcceptedWithEmptyProperties() {
    // Config construction stays lenient: the misconfiguration is only detected in open(), where
    // the producer is skipped (asserted in ProtoRowDataDeserializationSchemaNegativeConfigTest).
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("dlq", config.deadLetterTopic);
    assertTrue(config.getDeadLetterProperties().isEmpty());
  }

  @Test
  void deadLetter_propertiesAreKeptSeparateFromRegistryProperties() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    options.set(
        ProtoConfluentFormatOptions.DEAD_LETTER_PROPERTIES,
        Map.of("bootstrap.servers", "kafka:9092", "security.protocol", "SSL"));
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("kafka:9092", config.getDeadLetterProperties().get("bootstrap.servers"));
    assertEquals("SSL", config.getDeadLetterProperties().get("security.protocol"));
    // Dead-letter producer properties must not leak into the Schema Registry client properties.
    assertNull(config.getProperties().get("bootstrap.servers"));
    assertNull(config.getProperties().get("security.protocol"));
  }
}
