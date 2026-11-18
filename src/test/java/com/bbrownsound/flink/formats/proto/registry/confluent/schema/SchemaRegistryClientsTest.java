package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
  void createsAClient() {
    assertNotNull(SchemaRegistryClients.create("http://localhost:1", Map.of()));
  }
}
