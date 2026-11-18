package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Projection pushdown into the source (GitHub issue #173): the decoder produces, and converts, only
 * the columns the query reads.
 */
class ProtoDecodingFormatProjectionTest {

  private static final int SCHEMA_ID = 3;
  private static final String JSON = "application/vnd.schemaregistry.v1+json";

  /**
   * A table over SimpleMessage whose {@code date_time} column is declared with a type the format
   * cannot convert: building its converter throws, so any record that converts it fails.
   */
  private static final DataType WIDE_TABLE =
      DataTypes.ROW(
          DataTypes.FIELD("content", DataTypes.STRING()),
          DataTypes.FIELD("date_time", DataTypes.TIMESTAMP(3)),
          DataTypes.FIELD("added_later", DataTypes.STRING()));

  private HttpServer server;
  private final List<DataType> producedTypes = new ArrayList<>();

  /** A planner context that records the types the decoder asks type information for. */
  private final DynamicTableSource.Context context =
      new DynamicTableSource.Context() {
        @Override
        public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
          producedTypes.add(producedDataType);
          return null;
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
          return null;
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(
            DataType producedDataType) {
          return null;
        }
      };

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  /** Answers the schema-by-ID lookup with the SimpleMessage schema, anything else with a 404. */
  private void handle(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    boolean known = exchange.getRequestURI().getPath().equals("/schemas/ids/" + SCHEMA_ID);
    String body =
        known
            ? new ObjectMapper()
                .writeValueAsString(
                    Map.of(
                        "schemaType",
                        "PROTOBUF",
                        "schema",
                        new ProtobufSchema(TestSimple.SimpleMessage.getDescriptor())
                            .canonicalString()))
            : "{\"error_code\":40403,\"message\":\"Schema not found\"}";
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", JSON);
    exchange.sendResponseHeaders(known ? 200 : 404, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private ProtoDecodingFormat format(Map<String, String> extra) {
    String url = "http://127.0.0.1:" + server.getAddress().getPort();
    Map<String, String> props = new HashMap<>(extra);
    props.put("schema.registry.url", url);
    return new ProtoDecodingFormat(
        new ProtoConfluentFormatConfig(url, "wide-topic", false, props),
        getClass().getClassLoader());
  }

  /** Confluent wire format: magic byte, schema ID, message index (first message), payload. */
  private static byte[] record() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(0);
    out.writeInt(SCHEMA_ID);
    out.writeByte(0);
    out.write(
        TestSimple.SimpleMessage.newBuilder()
            .setContent("hello")
            .setDateTime("2026-01-01")
            .build()
            .toByteArray());
    return bytes.toByteArray();
  }

  @Test
  void isProjectableAtTheTopLevelOnly() {
    ProtoDecodingFormat format = format(Map.of());
    assertInstanceOf(ProjectableDecodingFormat.class, format);
    assertFalse(format.supportsNestedProjection());
  }

  @Test
  void decoderProducesTheProjectedColumnsInProjectionOrder() {
    format(Map.of()).createRuntimeDecoder(context, WIDE_TABLE, new int[][] {{2}, {0}});
    assertEquals(
        DataTypes.ROW(
            DataTypes.FIELD("added_later", DataTypes.STRING()),
            DataTypes.FIELD("content", DataTypes.STRING())),
        producedTypes.get(0));
  }

  @Test
  void decoderWithoutProjectionProducesEveryColumn() {
    format(Map.of()).createRuntimeDecoder(context, WIDE_TABLE);
    assertEquals(WIDE_TABLE, producedTypes.get(0));
  }

  @Test
  void unprojectedFieldsAreNotConverted() throws Exception {
    DeserializationSchema<RowData> projected =
        format(Map.of()).createRuntimeDecoder(context, WIDE_TABLE, new int[][] {{0}});
    projected.open(null);
    RowData row = projected.deserialize(record());
    assertEquals(1, row.getArity());
    assertEquals("hello", row.getString(0).toString());

    // Reading every column converts date_time too, which this table cannot do.
    DeserializationSchema<RowData> full = format(Map.of()).createRuntimeDecoder(context, WIDE_TABLE);
    full.open(null);
    IOException e = assertThrows(IOException.class, () -> full.deserialize(record()));
    assertTrue(e.getMessage().contains("TIMESTAMP_WITHOUT_TIME_ZONE"), e.getMessage());
  }

  @Test
  void messageClassIsCheckedAgainstTheProjectedColumns() {
    ProtoDecodingFormat format =
        format(Map.of("value.message-class", TestSimple.SimpleMessage.class.getName()));
    // 'added_later' has no field in SimpleMessage; a query that does not read it is accepted.
    format.createRuntimeDecoder(context, WIDE_TABLE, new int[][] {{0}});
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> format.createRuntimeDecoder(context, WIDE_TABLE, new int[][] {{0}, {2}}));
    assertTrue(e.getMessage().contains("column 'added_later' has no field"), e.getMessage());
  }
}
