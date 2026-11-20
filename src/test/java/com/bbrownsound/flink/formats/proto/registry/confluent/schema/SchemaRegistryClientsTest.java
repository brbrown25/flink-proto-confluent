package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SchemaRegistryClientsTest {

  @Test
  void mapsConnectionOptionsAndForwardsProperties() {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("url", "http://ignored");
    options.put("ssl.truststore.location", "/trust.jks");
    options.put("basic-auth.user-info", "user:secret");
    options.put("properties.max.schemas.per.subject", "10");
    options.put("properties.basic.auth.user.info", "overridden");

    Map<String, String> properties = SchemaRegistryClients.clientProperties(options);

    assertEquals(
        Map.of(
            "schema.registry.ssl.truststore.location", "/trust.jks",
            "basic.auth.user.info", "user:secret",
            "max.schemas.per.subject", "10"),
        properties,
        "a named connection option wins over the same client property passed through properties.");
  }

  @Test
  void everyConnectionOptionHasAClientProperty() {
    assertEquals(8, SchemaRegistryClients.CONNECTION_OPTIONS.size());
    SchemaRegistryClients.CONNECTION_OPTIONS.forEach(
        (option, property) -> assertEquals(property, SchemaRegistryClients.clientProperty(option)));
    assertTrue(
        SchemaRegistryClients.CONNECTION_OPTIONS.keySet()
            .containsAll(SchemaRegistryClients.SECRET_OPTIONS));
    assertThrows(IllegalArgumentException.class, () -> SchemaRegistryClients.clientProperty("url"));
  }

  @Test
  void stripsUserInfoFromRegistryUrls() {
    assertEquals("https://sr:8081", SchemaRegistryClients.withoutUserInfo("https://u:pw@sr:8081"));
    assertEquals(
        "https://sr:8081/ctx?a=b@c#d@e",
        SchemaRegistryClients.withoutUserInfo("https://u:p@ss@sr:8081/ctx?a=b@c#d@e"),
        "the user info ends at the last '@' before the path; later '@'s are kept");
    assertEquals(
        "http://h1:8081, http://h2:8081,",
        SchemaRegistryClients.withoutUserInfo("http://a:b@h1:8081, http://c:d@h2:8081,"),
        "each URL of a comma-separated list");
    assertEquals("sr:8081", SchemaRegistryClients.withoutUserInfo("u:pw@sr:8081"));
    assertEquals("http://sr:8081/", SchemaRegistryClients.withoutUserInfo("http://sr:8081/"));
    assertEquals("http://sr:8081", SchemaRegistryClients.withoutUserInfo("http://sr:8081"));
    assertNull(SchemaRegistryClients.withoutUserInfo(null));
  }

  @Test
  void createsAClient() {
    assertNotNull(SchemaRegistryClients.create("http://localhost:1", Map.of()));
  }
}
