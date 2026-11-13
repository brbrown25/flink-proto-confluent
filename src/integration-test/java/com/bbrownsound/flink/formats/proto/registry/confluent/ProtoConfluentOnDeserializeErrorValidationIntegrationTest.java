package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Integration test for GitHub issue #170, driven through SQL DDL and the real Kafka connector: an
 * unknown {@code on-deserialize-error} value (for example the typo {@code fial}) fails the query at
 * planning time with a {@link ValidationException} that lists the allowed values, instead of being
 * accepted and silently behaving as {@code skip}.
 *
 * <p>The format is resolved while the planner builds the table source, before anything connects to
 * Kafka or the Schema Registry, so this test needs no containers.
 */
@DisplayName("Proto-confluent on-deserialize-error validation through SQL")
class ProtoConfluentOnDeserializeErrorValidationIntegrationTest {

  private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

  private final TableEnvironment tableEnv =
      TableEnvironment.create(EnvironmentSettings.inStreamingMode());

  @ParameterizedTest(name = "[{index}] {0} = ''{1}''")
  @CsvSource({
    "on-deserialize-error,fial",
    "on-deserialize-error,''",
    "on-deserialize-error,skipp",
    "on_deserialize_error,fial",
    "on_deserialize_error,''",
    "on_deserialize_error,skipp"
  })
  void unknownValue_failsPlanningWithAllowedValues(String key, String value) {
    String table = createSourceTable("  'value.proto-confluent." + key + "' = '" + value + "',");

    ValidationException e =
        assertThrows(
            ValidationException.class, () -> tableEnv.explainSql("SELECT * FROM " + table));

    String message = findMessageInCauseChain(e, "Allowed values");
    assertNotNull(message, "No cause lists the allowed values: " + stackOfMessages(e));
    assertTrue(message.contains("'on-deserialize-error'"), message);
    assertTrue(message.contains("'" + value + "'"), message);
    assertTrue(message.contains("fail, skip"), message);
  }

  @ParameterizedTest(name = "[{index}] {0} = ''{1}''")
  @CsvSource({
    "on-deserialize-error,fail",
    "on-deserialize-error,FAIL",
    "on-deserialize-error,Fail",
    "on-deserialize-error,skip",
    "on-deserialize-error,SKIP",
    "on-deserialize-error,Skip",
    "on_deserialize_error,fail",
    "on_deserialize_error,FAIL",
    "on_deserialize_error,Fail",
    "on_deserialize_error,skip",
    "on_deserialize_error,SKIP",
    "on_deserialize_error,Skip"
  })
  void knownValue_plansInAnyCase(String key, String value) {
    String table = createSourceTable("  'value.proto-confluent." + key + "' = '" + value + "',");
    assertNotNull(tableEnv.explainSql("SELECT * FROM " + table));
  }

  @Test
  void unset_plans() {
    String table = createSourceTable("");
    assertNotNull(tableEnv.explainSql("SELECT * FROM " + table));
  }

  @Test
  void unknownValue_withDeadLetterTopic_stillFailsPlanning() {
    String table =
        createSourceTable(
            "  'value.proto-confluent.on-deserialize-error' = 'skipp',"
                + "  'value.proto-confluent.dead-letter-topic' = 'dlq',"
                + "  'value.proto-confluent.dead-letter.properties' ="
                + " 'bootstrap.servers:''localhost:9092''',");

    ValidationException e =
        assertThrows(
            ValidationException.class, () -> tableEnv.explainSql("SELECT * FROM " + table));
    String message = findMessageInCauseChain(e, "Allowed values");
    assertNotNull(message, "No cause lists the allowed values: " + stackOfMessages(e));
    assertTrue(message.contains("'skipp'"), message);
  }

  private String createSourceTable(String extraOptionLines) {
    String table = "on_error_src_" + TABLE_COUNTER.incrementAndGet();
    tableEnv.executeSql(
        "CREATE TABLE "
            + table
            + " ("
            + "  `content` STRING,"
            + "  `date_time` STRING"
            + ") WITH ("
            + "  'connector' = 'kafka',"
            + "  'topic' = 'on-error-validation',"
            + "  'properties.bootstrap.servers' = 'localhost:9092',"
            + "  'scan.startup.mode' = 'earliest-offset',"
            + "  'value.format' = 'proto-confluent',"
            + "  'value.proto-confluent.url' = 'http://localhost:8081',"
            + extraOptionLines
            + "  'value.proto-confluent.topic' = 'on-error-validation'"
            + ")");
    return table;
  }

  private static String findMessageInCauseChain(Throwable error, String fragment) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof ValidationException
          && t.getMessage() != null
          && t.getMessage().contains(fragment)) {
        return t.getMessage();
      }
    }
    return null;
  }

  private static String stackOfMessages(Throwable error) {
    StringBuilder out = new StringBuilder();
    for (Throwable t = error; t != null; t = t.getCause()) {
      out.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" | ");
    }
    return out.toString();
  }
}
