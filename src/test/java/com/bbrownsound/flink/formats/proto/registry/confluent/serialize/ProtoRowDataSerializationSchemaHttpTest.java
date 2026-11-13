package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.SUBJECT;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.schemaIdOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The sink with the real {@code CachedSchemaRegistryClient} against a local HTTP server that
 * answers the way a registry, or whatever stands in front of one, answers. This pins down how the
 * Confluent client in this build turns HTTP responses into error codes, which the stubbed tests
 * take for granted: a registry error body carries its own {@code error_code}, and a 404 without one
 * comes out as error code 50005 (unparseable body) or 404, never 40401.
 */
class ProtoRowDataSerializationSchemaHttpTest {

  private static final RowType ROW_TYPE = rowType("content", "date_time");
  private static final String LATEST_PATH = "/subjects/" + SUBJECT + "/versions/latest";
  private static final String JSON = "application/vnd.schemaregistry.v1+json";

  private HttpServer server;
  private final List<String> requests = new CopyOnWriteArrayList<>();
  private volatile int lookupStatus;
  private volatile String lookupBody;
  private volatile String lookupContentType;

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

  private void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    String path = exchange.getRequestURI().getPath();
    requests.add(method + " " + path);
    exchange.getRequestBody().readAllBytes();
    if ("GET".equals(method) && LATEST_PATH.equals(path)) {
      respond(exchange, lookupStatus, lookupContentType, lookupBody);
    } else if ("POST".equals(method) && path.equals("/subjects/" + SUBJECT + "/versions")) {
      respond(exchange, 200, JSON, "{\"id\":7}");
    } else {
      respond(exchange, 404, JSON, "{\"error_code\":40403,\"message\":\"Schema not found\"}");
    }
  }

  private static void respond(HttpExchange exchange, int status, String contentType, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", contentType);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private ProtoRowDataSerializationSchema openSink(boolean autoRegister) {
    Configuration options = new Configuration();
    options.set(
        ProtoConfluentFormatOptions.URL, "http://127.0.0.1:" + server.getAddress().getPort());
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, autoRegister);
    // The client retries 5xx answers (three more attempts by default) before it gives up; keep
    // the retries, drop the one-second waits between them.
    options.set(
        ProtoConfluentFormatOptions.PROPERTIES,
        Map.of("retries.wait.ms", "1", "retries.max.wait.ms", "5"));
    ProtoRowDataSerializationSchema schema =
        new ProtoRowDataSerializationSchema(ROW_TYPE, new ProtoConfluentFormatConfig(options));
    schema.open(null);
    return schema;
  }

  private boolean registrationAttempted() {
    return requests.contains("POST /subjects/" + SUBJECT + "/versions");
  }

  /** HTTP status, content type, body, and the status and error code the client must report. */
  static Stream<Arguments> errorResponses() {
    return Stream.of(
            Arguments.of(401, JSON, "{\"error_code\":401,\"message\":\"Unauthorized\"}", 401),
            Arguments.of(403, JSON, "{\"error_code\":40301,\"message\":\"Forbidden\"}", 40301),
            Arguments.of(
                500, JSON, "{\"error_code\":50001,\"message\":\"Error in the backend\"}", 50001),
            Arguments.of(503, "text/html", "<html>Service Unavailable</html>", 50005),
            // "Version not found" is not "subject not found": the subject is there.
            Arguments.of(
                404, JSON, "{\"error_code\":40402,\"message\":\"Version not found.\"}", 40402),
            // 404s that are not the registry saying "subject not found".
            Arguments.of(404, "text/html", "<html><body>404 Not Found</body></html>", 50005),
            Arguments.of(
                404, JSON, "{\"error_code\":404,\"message\":\"HTTP 404 Not Found\"}", 404),
            Arguments.of(404, "text/plain", "", 50005))
        .flatMap(
            response ->
                Stream.of(true, false)
                    .map(
                        autoRegister ->
                            Arguments.of(
                                response.get()[0],
                                response.get()[1],
                                response.get()[2],
                                response.get()[3],
                                autoRegister)));
  }

  @ParameterizedTest(name = "HTTP {0} {1} -> error code {3}, auto-register={4}")
  @MethodSource("errorResponses")
  void serialize_whenRegistryAnswersWithAnError_failsAndSendsNoRegistration(
      int status, String contentType, String body, int expectedErrorCode, boolean autoRegister) {
    lookupStatus = status;
    lookupContentType = contentType;
    lookupBody = body;
    ProtoRowDataSerializationSchema schema = openSink(autoRegister);

    SerializationException thrown =
        assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));

    RestClientException cause = assertInstanceOf(RestClientException.class, thrown.getCause());
    assertEquals(status, cause.getStatus());
    assertEquals(expectedErrorCode, cause.getErrorCode());
    assertTrue(thrown.getMessage().contains("HTTP status " + status), thrown.getMessage());
    assertFalse(requests.isEmpty());
    assertTrue(
        requests.stream().allMatch(("GET " + LATEST_PATH)::equals),
        "only the lookup (and the client's retries of it) may reach the registry: " + requests);
    assertFalse(registrationAttempted());
  }

  @Test
  void serialize_whenRegistryAnswersSubjectNotFound_andAutoRegister_registers() {
    lookupStatus = 404;
    lookupContentType = JSON;
    lookupBody = "{\"error_code\":40401,\"message\":\"Subject '" + SUBJECT + "' not found.\"}";
    ProtoRowDataSerializationSchema schema = openSink(true);

    byte[] bytes = schema.serialize(row("a", "b"));

    assertEquals(7, schemaIdOf(bytes), "the ID the registry assigned on registration");
    assertEquals(
        List.of("GET " + LATEST_PATH, "POST /subjects/" + SUBJECT + "/versions"), requests);
  }

  @Test
  void serialize_whenRegistryAnswersSubjectNotFound_andNoAutoRegister_failsWithoutRegistering() {
    lookupStatus = 404;
    lookupContentType = JSON;
    lookupBody = "{\"error_code\":40401,\"message\":\"Subject '" + SUBJECT + "' not found.\"}";
    ProtoRowDataSerializationSchema schema = openSink(false);

    SerializationException thrown =
        assertThrows(SerializationException.class, () -> schema.serialize(row("a", "b")));

    assertInstanceOf(RestClientException.class, thrown.getCause());
    assertFalse(registrationAttempted());
    assertEquals("GET " + LATEST_PATH, requests.get(0));
  }
}
