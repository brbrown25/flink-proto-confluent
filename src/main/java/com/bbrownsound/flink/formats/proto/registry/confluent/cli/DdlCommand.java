package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistrySchemaResolver;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoDescriptors;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoSqlType;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToSqlType;
import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * The {@code ddl} command: resolves a protobuf schema and prints its Flink SQL column list or a
 * {@code CREATE TABLE} statement. Uses no Flink classes, so it runs from the shadow JAR alone.
 */
final class DdlCommand {

  static final String FORMAT_PREFIX = "value.proto-confluent.";

  /** Substrings of {@code --property} keys whose values are treated as credentials. */
  private static final List<String> SECRET_PROPERTY_MARKERS =
      List.of("password", "user.info", "token", "secret", "jaas");

  private final PrintStream out;
  private final PrintStream err;
  private final BiFunction<String, Map<String, String>, SchemaRegistryClient> clientFactory;

  DdlCommand(
      PrintStream out,
      PrintStream err,
      BiFunction<String, Map<String, String>, SchemaRegistryClient> clientFactory) {
    this.out = out;
    this.err = err;
    this.clientFactory = clientFactory;
  }

  /**
   * Runs the command.
   *
   * @param args the arguments after {@code ddl}
   * @return the exit status
   */
  int run(List<String> args) {
    DdlArguments arguments;
    try {
      arguments = DdlArguments.parse(args);
    } catch (UsageException e) {
      err.println("error: " + e.getMessage());
      err.println();
      err.println(DdlArguments.USAGE);
      return ProtoConfluentCli.EXIT_USAGE;
    }
    if (arguments.help) {
      out.println(DdlArguments.USAGE);
      return ProtoConfluentCli.EXIT_OK;
    }
    ProtoSqlType rowType;
    try {
      rowType = rowType(arguments);
    } catch (RuntimeException | IOException e) {
      err.println("error: " + e.getMessage());
      return ProtoConfluentCli.EXIT_FAILURE;
    }
    if (arguments.columnsOnly) {
      out.println(CreateTableStatement.columns(rowType));
    } else {
      out.println(
          CreateTableStatement.render(arguments.tableName(), rowType, tableOptions(arguments)));
    }
    return ProtoConfluentCli.EXIT_OK;
  }

  private ProtoSqlType rowType(DdlArguments arguments) throws IOException {
    Descriptor descriptor =
        arguments.subject != null ? fromRegistry(arguments) : fromMessageClass(arguments);
    ProtoSqlType rowType = ProtoToSqlType.toSqlType(descriptor);
    return arguments.strictNotNull ? rowType.toStrictNotNull(arguments.keepNullable) : rowType;
  }

  private Descriptor fromRegistry(DdlArguments arguments) {
    Map<String, String> clientProperties =
        SchemaRegistryClients.clientProperties(arguments.connectionOptions);
    SchemaRegistryClient client = clientFactory.apply(arguments.url, clientProperties);
    return new SchemaRegistrySchemaResolver(client)
        .resolve(arguments.subject, arguments.version, arguments.messageName)
        .getDescriptor();
  }

  /**
   * Loads the message class through a class loader over {@code --classpath}.
   *
   * @throws IOException if a {@code --classpath} entry is not a valid URL, or the class loader
   *     cannot be closed
   */
  private static Descriptor fromMessageClass(DdlArguments arguments) throws IOException {
    List<URL> urls = new ArrayList<>();
    for (String entry : arguments.classpathEntries()) {
      Path path = Paths.get(entry);
      if (!path.toFile().exists()) {
        throw new IllegalArgumentException("--classpath entry does not exist: " + entry);
      }
      urls.add(path.toUri().toURL());
    }
    try (URLClassLoader loader =
        new URLClassLoader(urls.toArray(new URL[0]), DdlCommand.class.getClassLoader())) {
      Class<?> messageClass = Class.forName(arguments.messageClass, true, loader);
      return ProtoDescriptors.descriptorOf(messageClass);
    } catch (ClassNotFoundException | LinkageError e) {
      throw new IllegalArgumentException(
          "Cannot load message class '"
              + arguments.messageClass
              + "' ("
              + e
              + "); add its JAR or class directory with --classpath. Nested classes are "
              + "named with '$', e.g. com.example.Orders$Order",
          e);
    }
  }

  private Map<String, String> tableOptions(DdlArguments arguments) {
    Map<String, String> options = new LinkedHashMap<>();
    options.put("connector", "kafka");
    options.put("topic", arguments.topic);
    if (arguments.bootstrapServers != null) {
      options.put("properties.bootstrap.servers", arguments.bootstrapServers);
    }
    options.put("value.format", "proto-confluent");
    options.put(FORMAT_PREFIX + "url", arguments.formatUrl());
    options.put(FORMAT_PREFIX + "topic", arguments.topic);
    List<String> leftOut = new ArrayList<>();
    for (Map.Entry<String, String> option : arguments.connectionOptions.entrySet()) {
      if (!arguments.includeSecrets && isSecret(option.getKey())) {
        leftOut.add(option.getKey());
      } else {
        options.put(FORMAT_PREFIX + option.getKey(), option.getValue());
      }
    }
    options.putAll(arguments.tableOptions);
    if (!leftOut.isEmpty()) {
      err.println(
          "note: left "
              + String.join(", ", leftOut)
              + " out of the statement because it holds a credential; add '"
              + FORMAT_PREFIX
              + "<option>' to the WITH clause yourself, or pass --include-secrets");
    }
    return options;
  }

  static boolean isSecret(String formatOption) {
    if (SchemaRegistryClients.SECRET_OPTIONS.contains(formatOption)) {
      return true;
    }
    if (!formatOption.startsWith(SchemaRegistryClients.PROPERTIES_PREFIX)) {
      return false;
    }
    String key = formatOption.toLowerCase(Locale.ROOT);
    return SECRET_PROPERTY_MARKERS.stream().anyMatch(key::contains);
  }
}
