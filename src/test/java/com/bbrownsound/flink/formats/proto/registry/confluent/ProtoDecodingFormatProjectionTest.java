package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoJson;
import com.bbrownsound.flink.formats.proto.test.v1.TestNestedProjection.NestedProjectionTest;
import com.bbrownsound.flink.formats.proto.test.v1.TestNestedProjection.NestedProjectionTest.Inner;
import com.bbrownsound.flink.formats.proto.test.v1.TestNestedProjection.NestedProjectionTest.Payload;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Projection pushdown into the source (GitHub issues #173 and #217): the decoder produces, and
 * converts, only the columns the query reads, including single fields of nested columns.
 */
class ProtoDecodingFormatProjectionTest {

  private static final int SCHEMA_ID = 3;
  private static final int NESTED_SCHEMA_ID = 4;
  private static final Map<Integer, Descriptor> SCHEMAS =
      Map.of(
          SCHEMA_ID,
          TestSimple.SimpleMessage.getDescriptor(),
          NESTED_SCHEMA_ID,
          NestedProjectionTest.getDescriptor());
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

  private static final DataType INNER = ROW(FIELD("x", STRING()), FIELD("y", INT()));

  /**
   * A table over NestedProjectionTest. {@code payload.f7} is a string field declared with a type the
   * format cannot convert, so any read that converts the whole {@code payload} column fails; {@code
   * payload.added_later} and the third case of {@code payload.kind} have no field in the message.
   */
  private static final DataType NESTED_TABLE =
      ROW(
          FIELD("id", STRING()),
          FIELD(
              "payload",
              ROW(
                  FIELD("a", BIGINT()),
                  FIELD("b", STRING()),
                  FIELD("c", INT()),
                  FIELD("inner", INNER),
                  FIELD(
                      "kind",
                      ROW(
                          FIELD("text", STRING()),
                          FIELD("nested", INNER),
                          FIELD("extra_case", STRING()))),
                  FIELD("f7", TIMESTAMP(3)),
                  FIELD("added_later", STRING()))),
          FIELD("choice", ROW(FIELD("code", INT()), FIELD("detail", INNER))),
          FIELD("note", STRING()),
          FIELD("items", ARRAY(INNER)));

  private static final int[] ID = {0};
  private static final int[] PAYLOAD = {1};
  private static final int[] PAYLOAD_A = {1, 0};
  private static final int[] PAYLOAD_B = {1, 1};
  private static final int[] PAYLOAD_C = {1, 2};
  private static final int[] PAYLOAD_INNER_X = {1, 3, 0};
  private static final int[] PAYLOAD_INNER_Y = {1, 3, 1};
  private static final int[] PAYLOAD_KIND = {1, 4};
  private static final int[] PAYLOAD_KIND_TEXT = {1, 4, 0};
  private static final int[] PAYLOAD_KIND_NESTED_X = {1, 4, 1, 0};
  private static final int[] PAYLOAD_KIND_EXTRA = {1, 4, 2};
  private static final int[] PAYLOAD_ADDED_LATER = {1, 6};
  private static final int[] CHOICE = {2};
  private static final int[] CHOICE_CODE = {2, 0};
  private static final int[] CHOICE_DETAIL_X = {2, 1, 0};
  private static final int[] NOTE = {3};
  private static final int[] ITEMS = {4};

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

  /** Answers the schema-by-ID lookup with the schemas in {@link #SCHEMAS}, anything else a 404. */
  private void handle(HttpExchange exchange) throws IOException {
    exchange.getRequestBody().readAllBytes();
    String path = exchange.getRequestURI().getPath();
    Descriptor schema =
        path.startsWith("/schemas/ids/")
            ? SCHEMAS.get(Integer.valueOf(path.substring("/schemas/ids/".length())))
            : null;
    boolean known = schema != null;
    String body =
        known
            ? new ObjectMapper()
                .writeValueAsString(
                    Map.of(
                        "schemaType", "PROTOBUF",
                        "schema", new ProtobufSchema(schema).canonicalString()))
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
    return record(
        SCHEMA_ID,
        TestSimple.SimpleMessage.newBuilder().setContent("hello").setDateTime("2026-01-01").build());
  }

  private static byte[] record(int schemaId, Message message) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(0);
    out.writeInt(schemaId);
    out.writeByte(0);
    out.write(message.toByteArray());
    return bytes.toByteArray();
  }

  /** A NestedProjectionTest with every field the nested tests read set. */
  private static NestedProjectionTest fullNested() {
    return NestedProjectionTest.newBuilder()
        .setId("order-1")
        .setPayload(
            Payload.newBuilder()
                .setA(42L)
                .setB("bee")
                .setInner(Inner.newBuilder().setX("inner-x").setY(7))
                .setText("some text")
                .setF7("not a timestamp"))
        .setDetail(Inner.newBuilder().setX("detail-x"))
        .setNote(StringValue.of("a note"))
        .addItems(Inner.newBuilder().setX("item-0"))
        .build();
  }

  /** Builds the nested-projection decoder over NESTED_TABLE, opens it and reads one record. */
  private RowData readNested(Map<String, String> extra, NestedProjectionTest message, int[]... paths)
      throws Exception {
    return readNested(NESTED_TABLE, extra, message, paths);
  }

  private RowData readNested(
      DataType table, Map<String, String> extra, NestedProjectionTest message, int[]... paths)
      throws Exception {
    DeserializationSchema<RowData> decoder =
        format(extra).createRuntimeDecoder(context, table, paths);
    decoder.open(null);
    return decoder.deserialize(record(NESTED_SCHEMA_ID, message));
  }

  @Test
  void isProjectableIncludingNestedFields() {
    ProtoDecodingFormat format = format(Map.of());
    assertInstanceOf(ProjectableDecodingFormat.class, format);
    assertTrue(format.supportsNestedProjection());
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

  @Test
  void nestedDecoderProducesOneColumnPerProjectedPath() {
    format(Map.of()).createRuntimeDecoder(context, NESTED_TABLE, new int[][] {PAYLOAD_A, ID});
    assertEquals(ROW(FIELD("payload_a", BIGINT()), FIELD("id", STRING())), producedTypes.get(0));
  }

  @Test
  void siblingNestedFieldsAreNotConverted() throws Exception {
    // payload.f7 cannot be converted: reading payload.a converts neither it nor any other sibling...
    RowData row = readNested(Map.of(), fullNested(), PAYLOAD_A);
    assertEquals(1, row.getArity());
    assertEquals(42L, row.getLong(0));

    // ... while reading the whole payload column converts f7 too, which this table cannot do.
    IOException e =
        assertThrows(IOException.class, () -> readNested(Map.of(), fullNested(), PAYLOAD));
    assertTrue(e.getMessage().contains("TIMESTAMP_WITHOUT_TIME_ZONE"), e.getMessage());
  }

  @Test
  void nestedProjectionReadsEachPathFromTheMessage() throws Exception {
    RowData row =
        readNested(
            Map.of(),
            fullNested(),
            PAYLOAD_INNER_X,
            ID,
            PAYLOAD_B,
            PAYLOAD_C,
            PAYLOAD_INNER_Y,
            PAYLOAD_KIND_TEXT,
            PAYLOAD_KIND_NESTED_X,
            CHOICE_DETAIL_X,
            CHOICE_CODE,
            NOTE,
            ITEMS,
            PAYLOAD_KIND,
            CHOICE);
    assertEquals(13, row.getArity());
    assertEquals("inner-x", row.getString(0).toString());
    assertEquals("order-1", row.getString(1).toString());
    assertEquals("bee", row.getString(2).toString());
    // An unset optional field is NULL, as it is when the whole column is read.
    assertTrue(row.isNullAt(3));
    assertEquals(7, row.getInt(4));
    // The set oneof case is read; a case that is not set is NULL.
    assertEquals("some text", row.getString(5).toString());
    assertTrue(row.isNullAt(6));
    assertEquals("detail-x", row.getString(7).toString());
    assertTrue(row.isNullAt(8));
    // A wrapper, an array and whole oneof columns next to the nested paths.
    assertEquals("a note", row.getString(9).toString());
    ArrayData items = row.getArray(10);
    assertEquals(1, items.size());
    assertEquals("item-0", items.getRow(0, 2).getString(0).toString());
    RowData kind = row.getRow(11, 3);
    assertEquals("some text", kind.getString(0).toString());
    assertTrue(kind.isNullAt(1));
    RowData choice = row.getRow(12, 2);
    assertTrue(choice.isNullAt(0));
    assertEquals("detail-x", choice.getRow(1, 2).getString(0).toString());
  }

  @Test
  void pathsThroughUnsetMessagesAndOneofsAreNull() throws Exception {
    // No payload and no choice: every column under them is NULL, as the whole columns are.
    RowData row =
        readNested(
            Map.of(),
            NestedProjectionTest.newBuilder().setId("bare").build(),
            PAYLOAD_A,
            PAYLOAD_INNER_X,
            PAYLOAD_KIND,
            PAYLOAD_KIND_NESTED_X,
            CHOICE_DETAIL_X,
            CHOICE,
            ID);
    for (int i = 0; i < 6; i++) {
      assertTrue(row.isNullAt(i), "column " + i);
    }
    assertEquals("bare", row.getString(6).toString());

    // A payload without its inner message: the path stops at the unset message.
    RowData inner =
        readNested(
            Map.of(),
            NestedProjectionTest.newBuilder().setPayload(Payload.newBuilder().setA(1L)).build(),
            PAYLOAD_INNER_X,
            PAYLOAD_A,
            PAYLOAD_KIND);
    assertTrue(inner.isNullAt(0));
    assertEquals(1L, inner.getLong(1));
    // No case of payload.kind is set: the oneof column is NULL, as in a full read.
    assertTrue(inner.isNullAt(2));
  }

  @Test
  void nestedColumnsWithoutAFieldReadAsNull() throws Exception {
    // A field the writer's message does not have, and a oneof case past its last case.
    RowData row = readNested(Map.of(), fullNested(), PAYLOAD_ADDED_LATER, PAYLOAD_KIND_EXTRA, ID);
    assertTrue(row.isNullAt(0));
    assertTrue(row.isNullAt(1));
    assertEquals("order-1", row.getString(2).toString());
  }

  @Test
  void nestedMessageDeclaredAsStringOrBytesIsReadWhole() throws Exception {
    // A message column declared STRING or BYTES is a leaf the planner cannot descend into: the
    // projected path ends there and reads the declared alternative.
    DataType table = ROW(FIELD("payload", ROW(FIELD("inner", STRING()))));
    Inner inner = Inner.newBuilder().setX("x").setY(3).build();
    NestedProjectionTest message =
        NestedProjectionTest.newBuilder().setPayload(Payload.newBuilder().setInner(inner)).build();
    RowData row = readNested(table, Map.of(), message, new int[] {0, 0});
    assertEquals(ProtoJson.toJson(inner), row.getString(0).toString());

    DataType bytesTable = ROW(FIELD("payload", ROW(FIELD("inner", BYTES()))));
    RowData bytes = readNested(bytesTable, Map.of(), message, new int[] {0, 0});
    assertArrayEquals(inner.toByteArray(), bytes.getBinary(0));
  }

  @Test
  void nestedPathThroughAFieldThatIsNotAMessageFailsToRead() throws Exception {
    // 'id' is a string and 'items' a repeated message: neither can be read as a ROW.
    DataType scalar = ROW(FIELD("id", ROW(FIELD("z", INT()))));
    IOException e =
        assertThrows(
            IOException.class, () -> readNested(scalar, Map.of(), fullNested(), new int[] {0, 0}));
    assertTrue(e.getMessage().contains("reads as a value, not a message"), e.getMessage());

    DataType repeated = ROW(FIELD("items", ROW(FIELD("x", STRING()))));
    e =
        assertThrows(
            IOException.class,
            () -> readNested(repeated, Map.of(), fullNested(), new int[] {0, 0}));
    assertTrue(e.getMessage().contains("reads as an ARRAY or a MAP"), e.getMessage());

    // With on-deserialize-error = skip the record is dropped instead of failing the source.
    String url = "http://127.0.0.1:" + server.getAddress().getPort();
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(url, "wide-topic", false, Map.of("schema.registry.url", url));
    config.onDeserializeError = OnDeserializeError.SKIP;
    DeserializationSchema<RowData> skipping =
        new ProtoDecodingFormat(config, getClass().getClassLoader())
            .createRuntimeDecoder(context, scalar, new int[][] {{0, 0}});
    skipping.open(null);
    assertNull(skipping.deserialize(record(NESTED_SCHEMA_ID, fullNested())));
  }

  @Test
  void messageClassIsCheckedAgainstTheProjectedNestedFields() throws Exception {
    Map<String, String> messageClass =
        Map.of("value.message-class", NestedProjectionTest.class.getName());
    ProtoDecodingFormat format = format(messageClass);

    // payload.f7, payload.added_later and payload.kind.extra_case do not match the message, but
    // this query reads none of them.
    format.createRuntimeDecoder(
        context,
        NESTED_TABLE,
        new int[][] {PAYLOAD_A, PAYLOAD_INNER_X, CHOICE, PAYLOAD_KIND_TEXT, CHOICE_CODE, ID});
    // The source checks the same columns again when it opens, and reads them.
    RowData row = readNested(messageClass, fullNested(), PAYLOAD_A, PAYLOAD_KIND_TEXT);
    assertEquals(42L, row.getLong(0));
    assertEquals("some text", row.getString(1).toString());

    ValidationException e =
        assertThrows(
            ValidationException.class,
            () ->
                format.createRuntimeDecoder(
                    context,
                    NESTED_TABLE,
                    new int[][] {PAYLOAD_A, PAYLOAD_ADDED_LATER, PAYLOAD_KIND_EXTRA}));
    assertTrue(
        e.getMessage().contains("column 'payload.added_later' has no field in message"),
        e.getMessage());
    assertTrue(
        e.getMessage().contains("column 'payload.kind.extra_case' has no case in oneof"),
        e.getMessage());

    // The leaf's type is checked as for a top-level column, and so is a ROW over a scalar field.
    DataType mismatched =
        ROW(FIELD("payload", ROW(FIELD("a", STRING()))), FIELD("id", ROW(FIELD("z", INT()))));
    e =
        assertThrows(
            ValidationException.class,
            () -> format.createRuntimeDecoder(context, mismatched, new int[][] {{0, 0}, {1, 0}}));
    assertTrue(e.getMessage().contains("column 'payload.a' is STRING"), e.getMessage());
    assertTrue(e.getMessage().contains("column 'id' is ROW<`z` INT>"), e.getMessage());

    // A whole oneof under a nested message is checked as a ROW of its cases.
    DataType oneofNotRow = ROW(FIELD("payload", ROW(FIELD("kind", STRING()))), FIELD("n", INT()));
    e =
        assertThrows(
            ValidationException.class,
            () -> format.createRuntimeDecoder(context, oneofNotRow, new int[][] {{0, 0}}));
    assertTrue(e.getMessage().contains("column 'payload.kind' is STRING"), e.getMessage());
  }
}
