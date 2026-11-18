package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Command line tools for the proto-confluent format. Run with {@code java -jar
 * flink-proto-confluent-<version>-all.jar <command> [options]}; the only command is {@code ddl},
 * which prints Flink SQL DDL for a protobuf schema in the Schema Registry or a compiled message
 * class.
 *
 * <p>The tools use no Flink classes, so they run from the shadow JAR alone.
 */
public final class ProtoConfluentCli {

  static final int EXIT_OK = 0;
  static final int EXIT_FAILURE = 1;
  static final int EXIT_USAGE = 2;

  static final String USAGE =
      String.join(
          "\n",
          "Usage: java -jar flink-proto-confluent-<version>-all.jar <command> [options]",
          "",
          "Commands:",
          "  ddl    print a Flink CREATE TABLE statement (or column list) for a protobuf",
          "         schema in the Schema Registry or a compiled message class",
          "",
          "Run '<command> --help' for the options of a command.");

  private final PrintStream out;
  private final PrintStream err;
  private final BiFunction<String, Map<String, String>, SchemaRegistryClient> clientFactory;

  /**
   * Creates a CLI that writes to the given streams and builds registry clients with the given
   * factory. Tests use this to run commands in-process and capture their output.
   *
   * @param out standard output
   * @param err standard error
   * @param clientFactory builds a registry client from a URL and client properties
   */
  ProtoConfluentCli(
      PrintStream out,
      PrintStream err,
      BiFunction<String, Map<String, String>, SchemaRegistryClient> clientFactory) {
    this.out = out;
    this.err = err;
    this.clientFactory = clientFactory;
  }

  /**
   * Creates a CLI that writes to the given streams and uses real registry clients.
   *
   * @param out standard output
   * @param err standard error
   */
  public ProtoConfluentCli(PrintStream out, PrintStream err) {
    this(out, err, SchemaRegistryClients::create);
  }

  /**
   * Runs a command and exits the JVM with its status.
   *
   * @param args the command and its options
   */
  public static void main(String[] args) {
    int status = new ProtoConfluentCli(System.out, System.err).run(args);
    System.out.flush();
    System.exit(status);
  }

  /**
   * Runs a command.
   *
   * @param args the command and its options
   * @return the exit status: 0 on success, 1 on failure, 2 on a usage error
   */
  public int run(String... args) {
    List<String> arguments = Arrays.asList(args);
    if (arguments.isEmpty()) {
      err.println(USAGE);
      return EXIT_USAGE;
    }
    String command = arguments.get(0);
    List<String> rest = arguments.subList(1, arguments.size());
    switch (command) {
      case "ddl":
        return new DdlCommand(out, err, clientFactory).run(rest);
      case "help":
      case "--help":
      case "-h":
        out.println(USAGE);
        return EXIT_OK;
      default:
        err.println("error: unknown command '" + command + "'");
        err.println();
        err.println(USAGE);
        return EXIT_USAGE;
    }
  }
}
