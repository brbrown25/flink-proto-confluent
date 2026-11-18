package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.List;
import java.util.Map;

/**
 * An order schema that imports a type from a second, separately registered schema: a nested
 * message, a repeated message, a map, a repeated scalar and a referenced type. Shared by the
 * resolver and {@code ddl} tests, in-process and against a real registry.
 */
public final class OrderSchemas {

  /** Import path of the referenced schema, as written in the order schema. */
  public static final String MONEY_IMPORT = "ddl/common/money.proto";

  public static final String MONEY_SCHEMA =
      String.join(
          "\n",
          "syntax = \"proto3\";",
          "package ddl.common;",
          "message Money {",
          "  string currency = 1;",
          "  int64 units = 2;",
          "}");

  public static final String ORDER_SCHEMA =
      String.join(
          "\n",
          "syntax = \"proto3\";",
          "package ddl.orders;",
          "import \"" + MONEY_IMPORT + "\";",
          "message Order {",
          "  string order_id = 1;",
          "  Customer customer = 2;",
          "  repeated LineItem items = 3;",
          "  map<string, string> attributes = 4;",
          "  ddl.common.Money total = 5;",
          "  repeated string tags = 6;",
          "  message Customer {",
          "    string name = 1;",
          "    string email = 2;",
          "  }",
          "  message LineItem {",
          "    string sku = 1;",
          "    int32 quantity = 2;",
          "    ddl.common.Money price = 3;",
          "  }",
          "}",
          "message OrderKey {",
          "  string order_id = 1;",
          "}");

  /** The columns {@code ddl} prints for {@code ddl.orders.Order}. */
  public static final String ORDER_COLUMNS =
      String.join(
          ",\n",
          "`order_id` STRING",
          "`customer` ROW<`name` STRING, `email` STRING>",
          "`items` ARRAY<ROW<`sku` STRING, `quantity` INT, `price` ROW<`currency` STRING, "
              + "`units` BIGINT>> NOT NULL> NOT NULL",
          "`attributes` MAP<STRING, STRING> NOT NULL",
          "`total` ROW<`currency` STRING, `units` BIGINT>",
          "`tags` ARRAY<STRING> NOT NULL");

  private OrderSchemas() {}

  /** Returns the order schema with its reference resolved locally. */
  public static ProtobufSchema order(String moneySubject, int moneyVersion) {
    return new ProtobufSchema(
        ORDER_SCHEMA,
        List.of(new SchemaReference(MONEY_IMPORT, moneySubject, moneyVersion)),
        Map.of(MONEY_IMPORT, MONEY_SCHEMA),
        null,
        null);
  }

  /**
   * Registers the money schema under {@code moneySubject} and the order schema, referencing it,
   * under {@code orderSubject}.
   *
   * @return the order schema ID
   */
  public static int register(SchemaRegistryClient client, String moneySubject, String orderSubject)
      throws Exception {
    client.register(moneySubject, new ProtobufSchema(MONEY_SCHEMA));
    int moneyVersion = client.getLatestSchemaMetadata(moneySubject).getVersion();
    return client.register(orderSubject, order(moneySubject, moneyVersion));
  }
}
