package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaRegistrySchemaResolverTest {

  private MockSchemaRegistryClient client;
  private SchemaRegistrySchemaResolver resolver;
  private int orderId;

  @BeforeEach
  void setUp() throws Exception {
    client =
        new MockSchemaRegistryClient(
            List.of(new ProtobufSchemaProvider(), new AvroSchemaProvider()));
    resolver = new SchemaRegistrySchemaResolver(client);
    orderId = OrderSchemas.register(client, "ddl-common-money", "orders-value");
  }

  @Test
  void resolvesTheLatestVersionWithReferences() {
    ResolvedProtoSchema resolved = resolver.resolve("orders-value");

    assertEquals("orders-value", resolved.getSubject());
    assertEquals(1, resolved.getVersion());
    assertEquals(orderId, resolved.getId());
    assertEquals("ddl.orders.Order", resolved.getMessageName());
    assertEquals(
        "ddl.common.Money",
        resolved.getDescriptor().findFieldByName("total").getMessageType().getFullName(),
        "the referenced type must be resolved");
    assertEquals(OrderSchemas.ORDER_COLUMNS, String.join(",\n", resolved.toSqlType().toColumnDefinitions()));
    assertNotNull(resolved.getSchema());
    assertSame(client, resolver.getClient());
  }

  @Test
  void resolvesAVersionAndAMessage() throws Exception {
    client.register(
        "orders-value",
        new ProtobufSchema(
            OrderSchemas.ORDER_SCHEMA.replace("string order_id = 1;\n  Customer", "string order_id = 1;\n  string note = 7;\n  Customer"),
            OrderSchemas.order("ddl-common-money", 1).references(),
            Map.of(OrderSchemas.MONEY_IMPORT, OrderSchemas.MONEY_SCHEMA),
            null,
            null));

    assertEquals(2, resolver.resolve("orders-value").getVersion());
    assertTrue(
        resolver.resolve("orders-value").getDescriptor().findFieldByName("note") != null,
        "latest is version 2");
    ResolvedProtoSchema v1 = resolver.resolve("orders-value", 1, null);
    assertEquals(1, v1.getVersion());
    assertEquals(null, v1.getDescriptor().findFieldByName("note"));

    ResolvedProtoSchema key = resolver.resolve("orders-value", 1, "ddl.orders.OrderKey");
    assertEquals("ddl.orders.OrderKey", key.getMessageName());
  }

  @Test
  void reportsAMissingMessage() {
    SchemaResolutionException e =
        assertThrows(
            SchemaResolutionException.class,
            () -> resolver.resolve("orders-value", null, "ddl.orders.Nope"));
    assertTrue(e.isNotFound());
    assertTrue(e.getMessage().contains("ddl.orders.Order, ddl.orders.OrderKey"), e.getMessage());
  }

  @Test
  void reportsAMissingSubjectOrVersion() {
    SchemaResolutionException subject =
        assertThrows(SchemaResolutionException.class, () -> resolver.resolve("nope-value"));
    assertTrue(subject.isNotFound(), subject.getMessage());
    assertTrue(subject.getMessage().contains("'nope-value'"), subject.getMessage());

    SchemaResolutionException version =
        assertThrows(
            SchemaResolutionException.class, () -> resolver.resolve("orders-value", 9, null));
    assertTrue(version.isNotFound(), version.getMessage());
    assertTrue(version.getMessage().contains("version 9"), version.getMessage());
  }

  @Test
  void rejectsANonProtobufSchema() throws Exception {
    client.register(
        "avro-value",
        new AvroSchema("{\"type\":\"record\",\"name\":\"A\",\"fields\":[]}"));
    SchemaResolutionException e =
        assertThrows(SchemaResolutionException.class, () -> resolver.resolve("avro-value"));
    assertFalse(e.isNotFound());
    assertTrue(e.getMessage().contains("AVRO, not PROTOBUF"), e.getMessage());
  }

  @Test
  void reportsAnUnreachableRegistry() {
    SchemaRegistrySchemaResolver unreachable =
        SchemaRegistrySchemaResolver.create("http://localhost:1", Map.of());
    SchemaResolutionException e =
        assertThrows(SchemaResolutionException.class, () -> unreachable.resolve("orders-value"));
    assertFalse(e.isNotFound());
    assertTrue(e.getMessage().startsWith("Cannot reach the Schema Registry"), e.getMessage());
  }

  @Test
  void reportsOtherRegistryErrorsAsFound() {
    SchemaRegistrySchemaResolver failing =
        new SchemaRegistrySchemaResolver(
            new MockSchemaRegistryClient() {
              @Override
              public SchemaMetadata getSchemaMetadata(String subject, int version)
                  throws RestClientException {
                throw new RestClientException("Internal error", 500, 50001);
              }
            });
    SchemaResolutionException e =
        assertThrows(SchemaResolutionException.class, () -> failing.resolve("a-value", 1, null));
    assertFalse(e.isNotFound());
    assertTrue(
        e.getMessage().startsWith("Cannot read subject 'a-value' version 1 from the Schema Registry"),
        e.getMessage());
  }

  @Test
  void reportsAMissingVersionAsNotFound() {
    SchemaRegistrySchemaResolver missing =
        new SchemaRegistrySchemaResolver(
            new MockSchemaRegistryClient() {
              @Override
              public SchemaMetadata getSchemaMetadata(String subject, int version)
                  throws RestClientException {
                throw new RestClientException("Version not found", 404, 40402);
              }
            });
    assertTrue(
        assertThrows(SchemaResolutionException.class, () -> missing.resolve("a-value", 3, null))
            .isNotFound());
  }

  @Test
  void rejectsAProtobufSubjectWhoseSchemaDoesNotParseAsProtobuf() {
    SchemaRegistrySchemaResolver mismatched =
        new SchemaRegistrySchemaResolver(
            new MockSchemaRegistryClient() {
              @Override
              public SchemaMetadata getLatestSchemaMetadata(String subject) {
                return new SchemaMetadata(7, 1, "PROTOBUF", List.of(), "syntax = \"proto3\";");
              }

              @Override
              public ParsedSchema getSchemaBySubjectAndId(String subject, int id) {
                return new AvroSchema("{\"type\":\"string\"}");
              }
            });
    SchemaResolutionException e =
        assertThrows(SchemaResolutionException.class, () -> mismatched.resolve("a-value"));
    assertFalse(e.isNotFound());
    assertEquals(
        "Schema of subject 'a-value' (latest version) did not parse as a protobuf schema",
        e.getMessage());
  }
}
