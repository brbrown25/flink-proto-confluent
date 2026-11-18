package com.bbrownsound.flink.formats.proto.registry.confluent.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.RegistryProtoFormatFactory;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.junit.jupiter.api.Test;

class SchemaRegistryCatalogFactoryTest {

  private static final ClassLoader LOADER = SchemaRegistryCatalogFactoryTest.class.getClassLoader();

  private static Catalog create(Map<String, String> options) {
    return FactoryUtil.createCatalog("sr", options, new Configuration(), LOADER);
  }

  @Test
  void createsTheCatalogFromCreateCatalogOptions() throws Exception {
    Catalog catalog =
        create(
            Map.of(
                "type", "proto-confluent",
                "url", "http://registry:8081",
                "default-database", "kafka",
                "key.enabled", "false",
                "key.fields-prefix", "k_",
                "basic-auth.user-info", "u:p",
                "properties.bootstrap.servers", "kafka:9092",
                "scan.startup.mode", "earliest-offset",
                "sink.delivery-guarantee", "at-least-once",
                "table.value.proto-confluent.on-deserialize-error", "skip"));
    SchemaRegistryCatalog registry = assertInstanceOf(SchemaRegistryCatalog.class, catalog);
    assertEquals("sr", registry.getName());
    assertEquals(List.of("kafka"), registry.listDatabases());
  }

  @Test
  void rejectsUnknownOptionsAndAMissingUrl() {
    ValidationException unknown =
        assertThrows(
            ValidationException.class,
            () ->
                create(
                    Map.of("type", "proto-confluent", "url", "http://r:8081", "topic", "orders")));
    assertTrue(rootMessages(unknown).contains("topic"), rootMessages(unknown));

    ValidationException missing =
        assertThrows(ValidationException.class, () -> create(Map.of("type", "proto-confluent")));
    assertTrue(rootMessages(missing).contains("url"), rootMessages(missing));
  }

  @Test
  void sharesItsIdentifierWithTheFormatWithoutAmbiguity() {
    assertInstanceOf(
        SchemaRegistryCatalogFactory.class,
        FactoryUtil.discoverFactory(LOADER, CatalogFactory.class, "proto-confluent"));
    assertInstanceOf(
        RegistryProtoFormatFactory.class,
        FactoryUtil.discoverFactory(LOADER, DeserializationFormatFactory.class, "proto-confluent"));
    assertInstanceOf(
        RegistryProtoFormatFactory.class,
        FactoryUtil.discoverFactory(LOADER, SerializationFormatFactory.class, "proto-confluent"));
    assertEquals(RegistryProtoFormatFactory.IDENTIFIER, SchemaRegistryCatalogFactory.IDENTIFIER);
  }

  private static String rootMessages(Throwable t) {
    StringBuilder sb = new StringBuilder();
    for (Throwable c = t; c != null; c = c.getCause()) {
      sb.append(c.getMessage()).append('\n');
    }
    return sb.toString();
  }
}
