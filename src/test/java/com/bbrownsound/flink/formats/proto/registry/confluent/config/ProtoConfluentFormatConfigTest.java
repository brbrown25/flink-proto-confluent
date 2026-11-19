package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import java.io.ObjectStreamClass;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;

class ProtoConfluentFormatConfigTest {

  /**
   * A config serialized by the released 1.2.0 JAR (url, topic "events", value side, one
   * property, on-deserialize-error SKIP, schema cache TTL 5 minutes).
   */
  private static final String CONFIG_1_2_0_BASE64 =
      "rO0ABXNyAFhjb20uYmJyb3duc291bmQuZmxpbmsuZm9ybWF0cy5wcm90by5yZWdpc3RyeS5jb25mbHVl" +
      "bnQuY29uZmlnLlByb3RvQ29uZmx1ZW50Rm9ybWF0Q29uZmln7Z3ue/oncaYCAAlMABJkZWFkTGV0dGVy" +
      "RGVsaXZlcnl0AFJMY29tL2Jicm93bnNvdW5kL2ZsaW5rL2Zvcm1hdHMvcHJvdG8vcmVnaXN0cnkvY29u" +
      "Zmx1ZW50L2NvbmZpZy9EZWFkTGV0dGVyRGVsaXZlcnk7TAAUZGVhZExldHRlclByb3BlcnRpZXN0AA9M" +
      "amF2YS91dGlsL01hcDtMAA9kZWFkTGV0dGVyVG9waWN0ABJMamF2YS9sYW5nL1N0cmluZztMAAVpc0tl" +
      "eXQAE0xqYXZhL2xhbmcvQm9vbGVhbjtMABJvbkRlc2VyaWFsaXplRXJyb3J0AFJMY29tL2Jicm93bnNv" +
      "dW5kL2ZsaW5rL2Zvcm1hdHMvcHJvdG8vcmVnaXN0cnkvY29uZmx1ZW50L2NvbmZpZy9PbkRlc2VyaWFs" +
      "aXplRXJyb3I7TAAKcHJvcGVydGllc3EAfgACTAAOc2NoZW1hQ2FjaGVUdGx0ABRMamF2YS90aW1lL0R1" +
      "cmF0aW9uO0wAEXNjaGVtYVJlZ2lzdHJ5VXJscQB+AANMAAV0b3BpY3EAfgADeHB+cgBQY29tLmJicm93" +
      "bnNvdW5kLmZsaW5rLmZvcm1hdHMucHJvdG8ucmVnaXN0cnkuY29uZmx1ZW50LmNvbmZpZy5EZWFkTGV0" +
      "dGVyRGVsaXZlcnkAAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51bQAAAAAAAAAAEgAAeHB0AAVBU1lO" +
      "Q3NyABFqYXZhLnV0aWwuSGFzaE1hcAUH2sHDFmDRAwACRgAKbG9hZEZhY3RvckkACXRocmVzaG9sZHhw" +
      "P0AAAAAAAAB3CAAAABAAAAAAeHBzcgARamF2YS5sYW5nLkJvb2xlYW7NIHKA1Zz67gIAAVoABXZhbHVl" +
      "eHAAfnIAUGNvbS5iYnJvd25zb3VuZC5mbGluay5mb3JtYXRzLnByb3RvLnJlZ2lzdHJ5LmNvbmZsdWVu" +
      "dC5jb25maWcuT25EZXNlcmlhbGl6ZUVycm9yAAAAAAAAAAASAAB4cQB+AAl0AARTS0lQc3EAfgAMP0AA" +
      "AAAAAAF3CAAAAAIAAAABdAAdYmFzaWMuYXV0aC5jcmVkZW50aWFscy5zb3VyY2V0AAlVU0VSX0lORk94" +
      "c3IADWphdmEudGltZS5TZXKVXYS6GyJIsgwAAHhwdw0BAAAAAAAAASwAAAAAeHQADmh0dHA6Ly9zcjo4" +
      "MDgxdAAGZXZlbnRz";

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
    assertEquals(OnDeserializeError.FAIL, config.onDeserializeError);
  }

  @Test
  void onDeserializeError_explicitConstructorDefaultsToFail() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://sr:8081", "events", false, Map.of());
    assertEquals(OnDeserializeError.FAIL, config.onDeserializeError);
  }

  @Test
  void onDeserializeError_invalidValueIsRejected() {
    // Issue #170: an unrecognized value used to be carried verbatim and to take the 'skip' path at
    // runtime. It is now rejected when the config is built, even without the factory in front.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.setString("on-deserialize-error", "bogus");
    ValidationException e =
        assertThrows(ValidationException.class, () -> new ProtoConfluentFormatConfig(options));
    assertTrue(e.getMessage().contains("on-deserialize-error"), e.getMessage());
    assertTrue(e.getMessage().contains("'bogus'"), e.getMessage());
    assertTrue(e.getMessage().contains("fail, skip"), e.getMessage());
  }

  @Test
  void onDeserializeError_invalidValueViaFallbackKeyIsRejected() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.setString("on_deserialize_error", "fial");
    ValidationException e =
        assertThrows(ValidationException.class, () -> new ProtoConfluentFormatConfig(options));
    assertTrue(e.getMessage().contains("'fial'"), e.getMessage());
    assertTrue(e.getMessage().contains("fail, skip"), e.getMessage());
  }

  @Test
  void onDeserializeError_textValuesAreCaseInsensitive() {
    for (String value : new String[] {"fail", "FAIL", "Fail"}) {
      assertEquals(OnDeserializeError.FAIL, configWithText("on-deserialize-error", value));
      assertEquals(OnDeserializeError.FAIL, configWithText("on_deserialize_error", value));
    }
    for (String value : new String[] {"skip", "SKIP", "Skip"}) {
      assertEquals(OnDeserializeError.SKIP, configWithText("on-deserialize-error", value));
      assertEquals(OnDeserializeError.SKIP, configWithText("on_deserialize_error", value));
    }
  }

  @Test
  void onDeserializeError_typedValueIsCarried() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, OnDeserializeError.SKIP);
    assertEquals(
        OnDeserializeError.SKIP, new ProtoConfluentFormatConfig(options).onDeserializeError);
  }

  @Test
  void onDeserializeError_primaryKeyWinsOverFallbackKey() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.setString("on-deserialize-error", "skip");
    options.setString("on_deserialize_error", "fail");
    assertEquals(
        OnDeserializeError.SKIP, new ProtoConfluentFormatConfig(options).onDeserializeError);
  }

  @Test
  void onDeserializeError_survivesJavaSerialization() throws Exception {
    // The config is Java-serialized into the job graph inside the (de)serialization schema.
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.setString("on-deserialize-error", "Skip");
    options.set(ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC, "dlq");
    ProtoConfluentFormatConfig copy =
        InstantiationUtil.clone(new ProtoConfluentFormatConfig(options));
    assertEquals(OnDeserializeError.SKIP, copy.onDeserializeError);
    assertEquals("dlq", copy.deadLetterTopic);
    assertEquals("events", copy.topic);

    ProtoConfluentFormatConfig defaults =
        InstantiationUtil.clone(
            new ProtoConfluentFormatConfig("http://sr:8081", "events", false, Map.of()));
    assertEquals(OnDeserializeError.FAIL, defaults.onDeserializeError);
  }

  private static OnDeserializeError configWithText(String key, String value) {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.setString(key, value);
    return new ProtoConfluentFormatConfig(options).onDeserializeError;
  }

  @Test
  void deadLetter_topicWithoutBootstrapServers_isAcceptedWithEmptyProperties() {
    // Config construction stays lenient: the format factory rejects the table (or fills the
    // brokers in from it, see DeadLetterValidationTest), and open() fails for a config built
    // without the factory (asserted in ProtoRowDataDeserializationSchemaNegativeConfigTest).
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

  @Test
  void messageName_absentByDefault() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertNull(config.messageName);
    assertNull(config.getMessageClass());
  }

  @Test
  void messageName_isCarriedAndSurvivesJavaSerialization() throws Exception {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.MESSAGE_NAME, "com.example.Order");
    ProtoConfluentFormatConfig config = new ProtoConfluentFormatConfig(options);
    assertEquals("com.example.Order", config.messageName);
    ProtoConfluentFormatConfig copy =
        InstantiationUtil.clone(config, getClass().getClassLoader());
    assertEquals("com.example.Order", copy.messageName);
  }

  @Test
  void messageName_emptyValueIgnored() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.setString("message-name", "");
    assertNull(new ProtoConfluentFormatConfig(options).messageName);
  }

  @Test
  void getMessageClass_isRoleScoped() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, "http://sr:8081");
    options.set(ProtoConfluentFormatOptions.TOPIC, "events");
    options.set(ProtoConfluentFormatOptions.MESSAGE_CLASS, "com.example.Foo$Bar");
    assertEquals("com.example.Foo$Bar", new ProtoConfluentFormatConfig(options).getMessageClass());
    options.set(ProtoConfluentFormatOptions.IS_KEY, true);
    assertEquals("com.example.Foo$Bar", new ProtoConfluentFormatConfig(options).getMessageClass());
  }

  @Test
  void getMessageClass_readsATunneledValue() {
    ProtoConfluentFormatConfig value =
        new ProtoConfluentFormatConfig(
            "http://sr:8081", "events", false, Map.of("value.message-class", "a.B$C"));
    assertEquals("a.B$C", value.getMessageClass());
    ProtoConfluentFormatConfig key =
        new ProtoConfluentFormatConfig(
            "http://sr:8081", "events", true, Map.of("value.message-class", "a.B$C"));
    assertNull(key.getMessageClass());
    ProtoConfluentFormatConfig empty =
        new ProtoConfluentFormatConfig(
            "http://sr:8081", "events", false, Map.of("value.message-class", ""));
    assertNull(empty.getMessageClass());
  }

  @Test
  void serialVersionUidStaysPinnedToRelease120() {
    assertEquals(
        -1324640499131780698L,
        ObjectStreamClass.lookup(ProtoConfluentFormatConfig.class).getSerialVersionUID());
  }

  @Test
  void deserializesAConfigSerializedByRelease120() throws Exception {
    ProtoConfluentFormatConfig config =
        InstantiationUtil.deserializeObject(
            Base64.getDecoder().decode(CONFIG_1_2_0_BASE64),
            ProtoConfluentFormatConfigTest.class.getClassLoader());
    assertEquals("http://sr:8081", config.schemaRegistryUrl);
    assertEquals("events", config.topic);
    assertFalse(config.isKey);
    assertEquals(OnDeserializeError.SKIP, config.onDeserializeError);
    assertEquals(Duration.ofMinutes(5), config.schemaCacheTtl);
    assertEquals(Map.of("basic.auth.credentials.source", "USER_INFO"), config.getProperties());
    assertNull(config.subject);
    assertNull(config.messageName);
  }
}
