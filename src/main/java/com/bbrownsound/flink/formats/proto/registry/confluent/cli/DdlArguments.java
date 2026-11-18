package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** The parsed arguments of the {@code ddl} command. */
final class DdlArguments {

  static final String VALUE_SUBJECT_SUFFIX = "-value";

  static final String USAGE =
      String.join(
          "\n",
          "Usage: java -jar flink-proto-confluent-<version>-all.jar ddl [options]",
          "",
          "Prints Flink SQL for a protobuf schema: a CREATE TABLE statement for the Kafka",
          "connector with the proto-confluent value format, or only its column list.",
          "",
          "Schema (one of):",
          "  --url <url> --subject <subject>  read the schema from the Schema Registry",
          "      --version <n|latest>         subject version (default: latest)",
          "      --message-name <name>        fully-qualified message to use (default: the",
          "                                   first message in the schema)",
          "  --message-class <class>          use a compiled message class",
          "      --classpath <path>           JARs or class directories to load it from,",
          "                                   separated by '" + java.io.File.pathSeparator + "'"
              + " (repeatable)",
          "",
          "Output:",
          "  --columns-only                   print only the column list",
          "  --table <name>                   table name (default: the topic)",
          "  --topic <topic>                  Kafka topic (default: the subject without its",
          "                                   '-value' suffix)",
          "  --bootstrap-servers <servers>    set 'properties.bootstrap.servers'",
          "  --option <key>=<value>           add or override a table option (repeatable),",
          "                                   e.g. scan.startup.mode=earliest-offset",
          "  --table-registry-url <url>       registry URL written into the statement when",
          "                                   Flink reaches it at another address (default: --url)",
          "  --include-secrets                write passwords, user info and tokens into the",
          "                                   statement (left out by default)",
          "",
          "Nullability:",
          "  --strict-not-null                make scalar columns NOT NULL (TIMESTAMP_LTZ, DATE,",
          "                                   TIME and ROW keep their nullability)",
          "  --keep-nullable <path>[,<path>]  with --strict-not-null, keep these fields",
          "                                   nullable, e.g. customer.email or items[].note",
          "                                   (repeatable)",
          "",
          "Schema Registry connection (same names as the format options):",
          "  --ssl.keystore.location <path>     --ssl.keystore.password <password>",
          "  --ssl.truststore.location <path>   --ssl.truststore.password <password>",
          "  --basic-auth.credentials-source <source>",
          "  --basic-auth.user-info <user:password>",
          "  --bearer-auth.credentials-source <source>",
          "  --bearer-auth.token <token>",
          "  --property <key>=<value>         any other registry client property (repeatable)",
          "",
          "Exit status: 0 on success, 1 if the schema cannot be resolved, 2 on a usage error.");

  /** Options that take a value, besides the registry connection options. */
  private static final Set<String> VALUE_OPTIONS =
      Set.of(
          "--url",
          "--subject",
          "--version",
          "--message-name",
          "--message-class",
          "--classpath",
          "--table",
          "--topic",
          "--bootstrap-servers",
          "--table-registry-url",
          "--option",
          "--property",
          "--keep-nullable");

  String url;
  String subject;
  Integer version;
  String messageName;
  String messageClass;
  final List<String> classpath = new ArrayList<>();
  boolean columnsOnly;
  String table;
  String topic;
  String bootstrapServers;
  String tableRegistryUrl;
  boolean includeSecrets;
  boolean strictNotNull;
  final Set<String> keepNullable = new LinkedHashSet<>();
  final Map<String, String> tableOptions = new LinkedHashMap<>();

  /**
   * Registry connection settings as format options by name: the connection options, and {@code
   * properties.<key>} for each {@code --property}.
   */
  final Map<String, String> connectionOptions = new LinkedHashMap<>();

  boolean help;

  private DdlArguments() {}

  /**
   * Parses and validates the arguments that follow {@code ddl}.
   *
   * @param args the arguments
   * @return the parsed arguments
   * @throws UsageException if the arguments are invalid
   */
  static DdlArguments parse(List<String> args) throws UsageException {
    DdlArguments parsed = new DdlArguments();
    for (int i = 0; i < args.size(); i++) {
      String arg = args.get(i);
      String name = arg;
      String inlineValue = null;
      int eq = arg.indexOf('=');
      if (arg.startsWith("--") && eq > 0) {
        name = arg.substring(0, eq);
        inlineValue = arg.substring(eq + 1);
      }
      if (isFlag(name)) {
        if (inlineValue != null) {
          throw new UsageException(name + " does not take a value");
        }
        parsed.setFlag(name);
        continue;
      }
      if (!name.startsWith("--")) {
        throw new UsageException("Unexpected argument '" + arg + "'");
      }
      if (!VALUE_OPTIONS.contains(name)
          && !SchemaRegistryClients.CONNECTION_OPTIONS.containsKey(name.substring(2))) {
        throw new UsageException("Unknown option " + name);
      }
      String value;
      if (inlineValue != null) {
        value = inlineValue;
      } else if (i + 1 < args.size()) {
        value = args.get(++i);
      } else {
        throw new UsageException(name + " needs a value");
      }
      parsed.setOption(name, value);
    }
    if (!parsed.help) {
      parsed.validate();
    }
    return parsed;
  }

  private static boolean isFlag(String name) {
    switch (name) {
      case "--help":
      case "-h":
      case "--columns-only":
      case "--include-secrets":
      case "--strict-not-null":
        return true;
      default:
        return false;
    }
  }

  private void setFlag(String name) {
    switch (name) {
      case "--columns-only":
        columnsOnly = true;
        break;
      case "--include-secrets":
        includeSecrets = true;
        break;
      case "--strict-not-null":
        strictNotNull = true;
        break;
      default:
        help = true;
        break;
    }
  }

  private void setOption(String name, String value) throws UsageException {
    if (value.isEmpty()) {
      throw new UsageException(name + " needs a non-empty value");
    }
    switch (name) {
      case "--url":
        url = value;
        break;
      case "--subject":
        subject = value;
        break;
      case "--version":
        version = parseVersion(value);
        break;
      case "--message-name":
        messageName = value;
        break;
      case "--message-class":
        messageClass = value;
        break;
      case "--classpath":
        classpath.add(value);
        break;
      case "--table":
        table = value;
        break;
      case "--topic":
        topic = value;
        break;
      case "--bootstrap-servers":
        bootstrapServers = value;
        break;
      case "--table-registry-url":
        tableRegistryUrl = value;
        break;
      case "--option":
        String[] option = keyValue(name, value);
        tableOptions.put(option[0], option[1]);
        break;
      case "--property":
        String[] property = keyValue(name, value);
        connectionOptions.put(SchemaRegistryClients.PROPERTIES_PREFIX + property[0], property[1]);
        break;
      case "--keep-nullable":
        for (String path : value.split(",")) {
          if (!path.trim().isEmpty()) {
            keepNullable.add(path.trim());
          }
        }
        break;
      default:
        // A connection option, e.g. --basic-auth.user-info (parse() rejected unknown names).
        connectionOptions.put(name.substring(2), value);
        break;
    }
  }

  private static Integer parseVersion(String value) throws UsageException {
    if ("latest".equals(value.toLowerCase(Locale.ROOT))) {
      return null;
    }
    try {
      int version = Integer.parseInt(value);
      if (version > 0) {
        return version;
      }
    } catch (NumberFormatException e) {
      // reported below
    }
    throw new UsageException(
        "--version must be 'latest' or a positive version number, got '" + value + "'");
  }

  private static String[] keyValue(String name, String value) throws UsageException {
    int eq = value.indexOf('=');
    if (eq <= 0) {
      throw new UsageException(name + " needs <key>=<value>, got '" + value + "'");
    }
    return new String[] {value.substring(0, eq), value.substring(eq + 1)};
  }

  private void validate() throws UsageException {
    if ((subject == null) == (messageClass == null)) {
      throw new UsageException("Pass either --subject (with --url) or --message-class");
    }
    if (subject != null) {
      if (url == null) {
        throw new UsageException("--subject needs --url");
      }
      if (!classpath.isEmpty()) {
        throw new UsageException("--classpath only applies to --message-class");
      }
    } else {
      if (version != null || messageName != null) {
        throw new UsageException("--version and --message-name only apply to --subject");
      }
    }
    if (!keepNullable.isEmpty() && !strictNotNull) {
      throw new UsageException("--keep-nullable only applies with --strict-not-null");
    }
    if (columnsOnly) {
      List<String> statementOnly = new ArrayList<>();
      addIf(statementOnly, table != null, "--table");
      addIf(statementOnly, topic != null, "--topic");
      addIf(statementOnly, bootstrapServers != null, "--bootstrap-servers");
      addIf(statementOnly, !tableOptions.isEmpty(), "--option");
      addIf(statementOnly, tableRegistryUrl != null, "--table-registry-url");
      addIf(statementOnly, includeSecrets, "--include-secrets");
      if (!statementOnly.isEmpty()) {
        throw new UsageException(
            String.join(", ", statementOnly) + " only apply to a CREATE TABLE statement, not "
                + "with --columns-only");
      }
      return;
    }
    if (url == null && tableRegistryUrl == null) {
      throw new UsageException(
          "A CREATE TABLE statement needs the registry URL for the format: pass --url or "
              + "--table-registry-url (or --columns-only)");
    }
    if (topic == null) {
      topic = topicFromSubject();
    }
  }

  private String topicFromSubject() throws UsageException {
    if (subject != null
        && subject.endsWith(VALUE_SUBJECT_SUFFIX)
        && subject.length() > VALUE_SUBJECT_SUFFIX.length()) {
      return subject.substring(0, subject.length() - VALUE_SUBJECT_SUFFIX.length());
    }
    if (subject != null) {
      throw new UsageException(
          "Cannot derive the topic from subject '"
              + subject
              + "' (expected '<topic>-value'); pass --topic");
    }
    throw new UsageException("A CREATE TABLE statement for --message-class needs --topic");
  }

  private static void addIf(List<String> list, boolean condition, String value) {
    if (condition) {
      list.add(value);
    }
  }

  /** The registry URL written into the statement. */
  String formatUrl() {
    return tableRegistryUrl != null ? tableRegistryUrl : url;
  }

  /** The table name: {@code --table}, or the topic. */
  String tableName() {
    return table != null ? table : topic;
  }

  /** Returns the classpath entries, split on the path separator. */
  List<String> classpathEntries() {
    List<String> entries = new ArrayList<>();
    for (String value : classpath) {
      for (String entry : value.split(java.io.File.pathSeparator)) {
        if (!entry.isEmpty()) {
          entries.add(entry);
        }
      }
    }
    return Collections.unmodifiableList(entries);
  }
}
