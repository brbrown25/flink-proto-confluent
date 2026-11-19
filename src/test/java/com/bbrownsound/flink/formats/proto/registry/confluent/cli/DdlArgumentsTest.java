package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.RecursionStrategy;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DdlArgumentsTest {

  private static DdlArguments parse(String... args) throws UsageException {
    return DdlArguments.parse(List.of(args));
  }

  private static String usageError(String... args) {
    return assertThrows(UsageException.class, () -> parse(args)).getMessage();
  }

  @Test
  void subjectDefaults() throws Exception {
    DdlArguments args = parse("--url", "http://sr:8081", "--subject", "orders-value");
    assertEquals("http://sr:8081", args.url);
    assertEquals("orders-value", args.subject);
    assertNull(args.version, "latest by default");
    assertEquals("orders", args.topic, "derived from <topic>-value");
    assertEquals("orders", args.tableName());
    assertEquals("http://sr:8081", args.formatUrl());
    assertFalse(args.columnsOnly);
    assertFalse(args.strictNotNull);
    assertFalse(args.includeSecrets);
    assertFalse(args.help);
  }

  @Test
  void everyOption() throws Exception {
    DdlArguments args =
        parse(
            "--url=http://localhost:8081",
            "--subject",
            "orders-value",
            "--version",
            "3",
            "--message-name",
            "ddl.orders.Order",
            "--table",
            "my_orders",
            "--topic",
            "orders.v1",
            "--bootstrap-servers",
            "kafka:9092",
            "--option",
            "scan.startup.mode=earliest-offset",
            "--option=properties.group.id=g=1",
            "--table-registry-url",
            "http://schema-registry:8081",
            "--include-secrets",
            "--strict-not-null",
            "--keep-nullable",
            "a, b.c,, ",
            "--keep-nullable=d[]",
            "--basic-auth.user-info",
            "u:p",
            "--ssl.truststore.location=/t.jks",
            "--property",
            "max.retries=3");
    assertEquals(3, args.version);
    assertEquals("ddl.orders.Order", args.messageName);
    assertEquals("my_orders", args.tableName());
    assertEquals("orders.v1", args.topic);
    assertEquals("kafka:9092", args.bootstrapServers);
    assertEquals(
        Map.of("scan.startup.mode", "earliest-offset", "properties.group.id", "g=1"),
        args.tableOptions);
    assertEquals("http://schema-registry:8081", args.formatUrl());
    assertTrue(args.includeSecrets);
    assertTrue(args.strictNotNull);
    assertEquals(Set.of("a", "b.c", "d[]"), args.keepNullable);
    assertEquals(
        Map.of(
            "basic-auth.user-info", "u:p",
            "ssl.truststore.location", "/t.jks",
            "properties.max.retries", "3"),
        args.connectionOptions);
  }

  @Test
  void versionLatestAndColumnsOnly() throws Exception {
    DdlArguments args =
        parse("--url", "u", "--subject", "not-a-value-subject", "--version", "LATEST", "--columns-only");
    assertNull(args.version);
    assertTrue(args.columnsOnly);
    assertNull(args.topic, "no topic is needed for a column list");
  }

  @Test
  void messageClassWithClasspath() throws Exception {
    DdlArguments args =
        parse(
            "--message-class",
            "com.example.Orders$Order",
            "--classpath",
            "a.jar" + File.pathSeparator + File.pathSeparator + "classes",
            "--classpath",
            "b.jar",
            "--topic",
            "orders",
            "--table-registry-url",
            "http://sr");
    assertEquals(List.of("a.jar", "classes", "b.jar"), args.classpathEntries());
    assertEquals("http://sr", args.formatUrl());
  }

  @Test
  void help() throws Exception {
    assertTrue(parse("--help").help);
    assertTrue(parse("-h").help);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "--url u --subject s-value --bogus x|Unknown option --bogus",
        "--url u --subject s-value stray|Unexpected argument 'stray'",
        "--url u --subject|--subject needs a value",
        "--url u --subject s-value --columns-only=yes|--columns-only does not take a value",
        "--url u --subject=|--subject needs a non-empty value",
        "--url u --subject s-value --version 0|--version must be 'latest' or a positive version"
            + " number, got '0'",
        "--url u --subject s-value --version x|--version must be 'latest' or a positive version"
            + " number, got 'x'",
        "--url u --subject s-value --option nokey|--option needs <key>=<value>, got 'nokey'",
        "--url u --subject s-value --property =v|--property needs <key>=<value>, got '=v'",
        "--url u|Pass either --subject (with --url) or --message-class",
        "--subject s-value --message-class C|Pass either --subject (with --url) or --message-class",
        "--subject s-value|--subject needs --url",
        "--url u --subject s-value --classpath x|--classpath only applies to --message-class",
        "--message-class C --version 2|--version and --message-name only apply to --subject",
        "--message-class C --message-name M|--version and --message-name only apply to --subject",
        "--url u --subject s-value --keep-nullable a|--keep-nullable only applies with"
            + " --strict-not-null",
        "--url u --subject s-value --columns-only --table t --option a=b|--table, --option only"
            + " apply to a CREATE TABLE statement, not with --columns-only",
        "--message-class C --topic t|A CREATE TABLE statement needs the registry URL for the"
            + " format: pass --url or --table-registry-url (or --columns-only)",
        "--url u --message-class C|A CREATE TABLE statement for --message-class needs --topic",
        "--url u --subject orders-key|Cannot derive the topic from subject 'orders-key' (expected"
            + " '<topic>-value'); pass --topic",
        "--url u --subject -value|Cannot derive the topic from subject '-value' (expected"
            + " '<topic>-value'); pass --topic",
      })
  void usageErrors(String args, String message) {
    assertEquals(message, usageError(args.split(" ")));
  }

  @Test
  void columnsOnlyRejectsEveryStatementOption() {
    assertEquals(
        "--table, --topic, --bootstrap-servers, --option, --table-registry-url, --include-secrets"
            + " only apply to a CREATE TABLE statement, not with --columns-only",
        usageError(
            "--message-class",
            "C",
            "--columns-only",
            "--table",
            "t",
            "--topic",
            "t",
            "--bootstrap-servers",
            "b",
            "--option",
            "a=b",
            "--table-registry-url",
            "u",
            "--include-secrets"));
  }

  @Test
  void recursiveMessageDefaultsToFailAndParsesEachMode() throws Exception {
    assertEquals(
        RecursionStrategy.FAIL,
        parse("--message-class", "C", "--columns-only").recursion);
    assertEquals(
        RecursionStrategy.BYTES,
        parse("--message-class", "C", "--columns-only", "--recursive-message", "bytes").recursion);
    assertEquals(
        RecursionStrategy.JSON,
        parse("--message-class", "C", "--columns-only", "--recursive-message=json").recursion);
    assertEquals(
        RecursionStrategy.row(3),
        parse("--message-class", "C", "--columns-only", "--recursive-message", "row:3").recursion);
  }

  @Test
  void rejectsAnInvalidRecursiveMessage() {
    assertTrue(
        usageError("--message-class", "C", "--columns-only", "--recursive-message", "row")
            .startsWith("'recursive-message' must be 'fail', 'bytes', 'json' or 'row:<depth>'"));
    assertTrue(
        usageError("--message-class", "C", "--columns-only", "--recursive-message", "row:0")
            .contains("at least 1"));
    assertTrue(DdlArguments.USAGE.contains("--recursive-message <mode>"));
  }
}
