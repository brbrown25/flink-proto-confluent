package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.catalog.SchemaRegistryCatalogFactory;
import com.bbrownsound.flink.formats.proto.registry.confluent.catalog.SchemaRegistryCatalogOptions;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.junit.jupiter.api.Test;

/**
 * Checks the option tables of the documentation against the {@link ConfigOption}s they describe
 * (issue #131), so that renaming an option, changing its default or adding one without updating
 * the docs fails the build.
 *
 * <ul>
 *   <li>The format table in {@code docs/content/reference/configuration.md} against the public
 *       static {@code ConfigOption} fields of {@link ProtoConfluentFormatOptions}.
 *   <li>The catalog table in {@code docs/content/how-to/catalog.md} against the public static
 *       {@code ConfigOption} fields, connection options and pass-through prefixes of {@link
 *       SchemaRegistryCatalogOptions}.
 * </ul>
 *
 * <p>For each option the table must have a row whose Option cell names its key; the Default cell
 * must be the default value as a code span, or text that is not a single code span when the option
 * has no default; and the fallback keys named after "Fallback key" in the description must be the
 * option's fallback keys. Rows naming a key that is not an option fail too.
 *
 * <p>The pages are read from {@code docs/content} under the working directory, which Gradle sets to
 * the project directory, or from the directory in the {@code docs.contentDir} system property.
 */
class OptionsDocumentationTest {

  private static final String FORMAT_HEADER = "| Option | Default | Description |";
  private static final String CATALOG_HEADER = "| Option | Required | Default | Description |";

  private static final Pattern CODE_SPAN = Pattern.compile("`([^`]+)`");
  private static final Pattern FALLBACK_KEYS =
      Pattern.compile(
          "[Ff]allback keys?:?\\s+((?:`[^`]+`(?:\\s*,\\s*|\\s+and\\s+|\\s+or\\s+)?)+)");
  private static final String REQUIRED = "*(required)*";

  @Test
  void formatTableMatchesTheFormatOptions() throws IOException {
    List<String> problems =
        checkFormatTable(
            read("reference/configuration.md"),
            staticOptions(ProtoConfluentFormatOptions.class),
            keys(new RegistryProtoFormatFactory().requiredOptions()));
    assertTrue(problems.isEmpty(), report("reference/configuration.md", problems));
  }

  @Test
  void catalogTableMatchesTheCatalogOptions() throws IOException {
    Map<String, ConfigOption<?>> options = staticOptions(SchemaRegistryCatalogOptions.class);
    options.put(CommonCatalogOptions.CATALOG_TYPE.key(), CommonCatalogOptions.CATALOG_TYPE);
    Set<String> required = keys(new SchemaRegistryCatalogFactory().requiredOptions());
    required.add(CommonCatalogOptions.CATALOG_TYPE.key());
    List<String> problems =
        checkCatalogTable(
            read("how-to/catalog.md"),
            options,
            required,
            passThroughPrefixes(),
            SchemaRegistryCatalogFactory.IDENTIFIER);
    assertTrue(problems.isEmpty(), report("how-to/catalog.md", problems));
  }

  /** Every option the factories accept is a public static field the tables are checked against. */
  @Test
  void factoryOptionsAreStaticFields() {
    RegistryProtoFormatFactory format = new RegistryProtoFormatFactory();
    Set<String> formatKeys = keys(format.requiredOptions());
    formatKeys.addAll(keys(format.optionalOptions()));
    assertEquals(
        new TreeSet<>(formatKeys),
        new TreeSet<>(staticOptions(ProtoConfluentFormatOptions.class).keySet()));

    SchemaRegistryCatalogFactory catalog = new SchemaRegistryCatalogFactory();
    Set<String> catalogKeys = keys(catalog.requiredOptions());
    catalogKeys.addAll(keys(catalog.optionalOptions()));
    assertEquals(
        new TreeSet<>(catalogKeys),
        new TreeSet<>(staticOptions(SchemaRegistryCatalogOptions.class).keySet()));
  }

  // The checks below run on made-up tables, so that a regression in the checker itself (one that
  // would let every divergence through) also fails.

  @Test
  void reportsAChangedDefault() {
    List<String> problems =
        checkFormatTable(
            table(FORMAT_HEADER, "| `is_key` | `true` | Key or value. |"),
            options(ProtoConfluentFormatOptions.IS_KEY),
            Set.of());
    assertEquals(1, problems.size(), problems.toString());
    assertTrue(problems.get(0).contains("default"), problems.toString());
  }

  @Test
  void reportsADefaultOnAnOptionWithoutOne() {
    List<String> problems =
        checkFormatTable(
            table(FORMAT_HEADER, "| `subject` | `orders-value` | Subject. |"),
            options(ProtoConfluentFormatOptions.SUBJECT),
            Set.of());
    assertEquals(1, problems.size(), problems.toString());
    assertTrue(problems.get(0).contains("no default"), problems.toString());
  }

  @Test
  void reportsARenamedAndAMissingOption() {
    List<String> problems =
        checkFormatTable(
            table(FORMAT_HEADER, "| `is-key` | `false` | Key or value. |"),
            options(ProtoConfluentFormatOptions.IS_KEY),
            Set.of());
    assertEquals(2, problems.size(), problems.toString());
    assertTrue(problems.stream().anyMatch(p -> p.contains("'is-key'")), problems.toString());
    assertTrue(problems.stream().anyMatch(p -> p.contains("'is_key'")), problems.toString());
  }

  @Test
  void reportsWrongFallbackKeys() {
    List<String> problems =
        checkFormatTable(
            table(
                FORMAT_HEADER,
                "| `dead-letter-topic` | *(none)* | Topic. Fallback key: `dead-letter_topic`. |",
                "| `message-class` | *(none)* | Class. |"),
            options(
                ProtoConfluentFormatOptions.DEAD_LETTER_TOPIC,
                ProtoConfluentFormatOptions.MESSAGE_CLASS),
            Set.of());
    assertEquals(2, problems.size(), problems.toString());
    assertTrue(problems.stream().allMatch(p -> p.contains("fallback")), problems.toString());
  }

  @Test
  void reportsARequiredOptionThatIsNotMarkedRequired() {
    List<String> problems =
        checkFormatTable(
            table(FORMAT_HEADER, "| `url` | *(none)* | URL. Fallback key: `schema-registry.url`. |"),
            options(ProtoConfluentFormatOptions.URL),
            Set.of(ProtoConfluentFormatOptions.URL.key()));
    assertEquals(1, problems.size(), problems.toString());
    assertTrue(problems.get(0).contains("required"), problems.toString());
  }

  @Test
  void reportsCatalogDivergences() {
    List<String> problems =
        checkCatalogTable(
            table(
                CATALOG_HEADER,
                "| `type` | yes | | `other`. |",
                "| `url` | no | | URL. |",
                "| `key.enabled` | no | `false` | Key columns. |",
                "| `sink.<option>` | no | | Sink options. |",
                "| `nope.<option>` | no | | Nothing. |"),
            options(
                CommonCatalogOptions.CATALOG_TYPE,
                SchemaRegistryCatalogOptions.URL,
                SchemaRegistryCatalogOptions.KEY_ENABLED),
            Set.of(CommonCatalogOptions.CATALOG_TYPE.key(), SchemaRegistryCatalogOptions.URL.key()),
            Set.of("sink.", "scan."),
            SchemaRegistryCatalogFactory.IDENTIFIER);
    assertEquals(5, problems.size(), problems.toString());
  }

  // ---------------------------------------------------------------------------------------------

  private static List<String> checkFormatTable(
      String markdown, Map<String, ConfigOption<?>> options, Set<String> required) {
    List<String> problems = new ArrayList<>();
    List<Map<String, String>> rows = parseTable(markdown, FORMAT_HEADER, problems);
    Set<String> documented = new LinkedHashSet<>();
    for (Map<String, String> row : rows) {
      List<ConfigOption<?>> rowOptions = rowOptions(row, options, documented, problems);
      for (ConfigOption<?> option : rowOptions) {
        String defaultCell = row.get("Default");
        if (required.contains(option.key())) {
          if (!REQUIRED.equals(defaultCell)) {
            problems.add(
                "'" + option.key() + "' is required, but its Default is '" + defaultCell + "'");
          }
        } else {
          checkDefault(option, defaultCell, problems);
        }
      }
      checkFallbackKeys(row, rowOptions, problems);
    }
    reportUndocumented(options.keySet(), documented, problems);
    return problems;
  }

  private static List<String> checkCatalogTable(
      String markdown,
      Map<String, ConfigOption<?>> options,
      Set<String> required,
      Set<String> prefixes,
      String identifier) {
    List<String> problems = new ArrayList<>();
    List<Map<String, String>> rows = parseTable(markdown, CATALOG_HEADER, problems);
    Set<String> documented = new LinkedHashSet<>();
    Set<String> documentedPrefixes = new LinkedHashSet<>();
    for (Map<String, String> row : rows) {
      List<String> keys = codeSpans(row.get("Option"));
      List<String> prefixKeys =
          keys.stream().filter(k -> k.contains("<")).collect(Collectors.toList());
      for (String key : prefixKeys) {
        String prefix = key.substring(0, key.indexOf('<'));
        if (prefixes.contains(prefix)) {
          documentedPrefixes.add(prefix);
        } else {
          problems.add("'" + key + "' documents a prefix '" + prefix + "' the catalog does not pass");
        }
      }
      if (!prefixKeys.isEmpty()) {
        continue;
      }
      List<ConfigOption<?>> rowOptions = rowOptions(row, options, documented, problems);
      for (ConfigOption<?> option : rowOptions) {
        String expectedRequired = required.contains(option.key()) ? "yes" : "no";
        if (!expectedRequired.equals(row.get("Required"))) {
          problems.add(
              "'"
                  + option.key()
                  + "' has Required '"
                  + row.get("Required")
                  + "', expected '"
                  + expectedRequired
                  + "'");
        }
        if (option.key().equals(CommonCatalogOptions.CATALOG_TYPE.key())) {
          if (!codeSpans(row.get("Description")).contains(identifier)) {
            problems.add("'type' does not name the catalog identifier `" + identifier + "`");
          }
          continue;
        }
        checkDefault(option, row.get("Default"), problems);
      }
      checkFallbackKeys(row, rowOptions, problems);
    }
    reportUndocumented(options.keySet(), documented, problems);
    for (String prefix : prefixes) {
      if (!documentedPrefixes.contains(prefix)) {
        problems.add("the pass-through prefix '" + prefix + "' has no row");
      }
    }
    return problems;
  }

  /** Returns the options a row names, recording each documented key and every unknown one. */
  private static List<ConfigOption<?>> rowOptions(
      Map<String, String> row,
      Map<String, ConfigOption<?>> options,
      Set<String> documented,
      List<String> problems) {
    List<ConfigOption<?>> rowOptions = new ArrayList<>();
    List<String> keys = codeSpans(row.get("Option"));
    if (keys.isEmpty()) {
      problems.add("a row names no option: " + row);
    }
    for (String key : keys) {
      ConfigOption<?> option = options.get(key);
      if (option == null) {
        problems.add("'" + key + "' is documented but is not an option");
      } else if (!documented.add(key)) {
        problems.add("'" + key + "' is documented in more than one row");
      } else {
        rowOptions.add(option);
      }
    }
    return rowOptions;
  }

  private static void checkDefault(ConfigOption<?> option, String cell, List<String> problems) {
    List<String> spans = codeSpans(cell);
    boolean singleSpan = spans.size() == 1 && cell.equals("`" + spans.get(0) + "`");
    if (option.hasDefaultValue()) {
      String expected = "`" + option.defaultValue() + "`";
      if (!expected.equals(cell)) {
        problems.add(
            "'" + option.key() + "' has default " + expected + ", but its Default is '" + cell + "'");
      }
    } else if (singleSpan) {
      problems.add(
          "'" + option.key() + "' has no default, but its Default is the value '" + cell + "'");
    }
  }

  private static void checkFallbackKeys(
      Map<String, String> row, List<ConfigOption<?>> rowOptions, List<String> problems) {
    Set<String> actual = new TreeSet<>();
    for (ConfigOption<?> option : rowOptions) {
      for (FallbackKey fallbackKey : option.fallbackKeys()) {
        if (!fallbackKey.getKey().equals(option.key())) {
          actual.add(fallbackKey.getKey());
        }
      }
    }
    Set<String> documented = new TreeSet<>();
    Matcher matcher = FALLBACK_KEYS.matcher(row.get("Description"));
    while (matcher.find()) {
      documented.addAll(codeSpans(matcher.group(1)));
    }
    if (!rowOptions.isEmpty() && !actual.equals(documented)) {
      problems.add(
          rowOptions.stream().map(o -> "'" + o.key() + "'").collect(Collectors.joining(", "))
              + " has fallback keys "
              + actual
              + ", but the docs name "
              + documented);
    }
  }

  private static void reportUndocumented(
      Set<String> optionKeys, Set<String> documented, List<String> problems) {
    for (String key : optionKeys) {
      if (!documented.contains(key)) {
        problems.add("'" + key + "' is an option but has no row");
      }
    }
  }

  /** Parses the table that starts with the given header line into rows keyed by column name. */
  private static List<Map<String, String>> parseTable(
      String markdown, String header, List<String> problems) {
    List<String> lines = markdown.lines().collect(Collectors.toList());
    int start = lines.indexOf(header);
    List<Map<String, String>> rows = new ArrayList<>();
    if (start < 0) {
      problems.add("no table with the header " + header);
      return rows;
    }
    List<String> columns = splitRow(header);
    for (int i = start + 2; i < lines.size() && lines.get(i).startsWith("|"); i++) {
      List<String> cells = splitRow(lines.get(i));
      if (cells.size() != columns.size()) {
        problems.add("row has " + cells.size() + " cells, expected " + columns.size() + ": " + lines.get(i));
        continue;
      }
      Map<String, String> row = new LinkedHashMap<>();
      for (int c = 0; c < columns.size(); c++) {
        row.put(columns.get(c), cells.get(c));
      }
      rows.add(row);
    }
    if (rows.isEmpty()) {
      problems.add("the table with the header " + header + " has no rows");
    }
    return rows;
  }

  /** Splits a table row on the pipes outside code spans, trimming each cell. */
  private static List<String> splitRow(String line) {
    String body = line.trim();
    body = body.substring(1, body.endsWith("|") ? body.length() - 1 : body.length());
    List<String> cells = new ArrayList<>();
    StringBuilder cell = new StringBuilder();
    boolean inCode = false;
    for (int i = 0; i < body.length(); i++) {
      char ch = body.charAt(i);
      if (ch == '`') {
        inCode = !inCode;
      }
      if (ch == '|' && !inCode && (i == 0 || body.charAt(i - 1) != '\\')) {
        cells.add(cell.toString().trim());
        cell.setLength(0);
      } else {
        cell.append(ch);
      }
    }
    cells.add(cell.toString().trim());
    return cells;
  }

  private static List<String> codeSpans(String text) {
    List<String> spans = new ArrayList<>();
    Matcher matcher = CODE_SPAN.matcher(text);
    while (matcher.find()) {
      spans.add(matcher.group(1));
    }
    return spans;
  }

  /** The public static options of a class: its ConfigOption fields and collections of them. */
  private static Map<String, ConfigOption<?>> staticOptions(Class<?> type) {
    Map<String, ConfigOption<?>> options = new LinkedHashMap<>();
    for (Field field : type.getFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      Object value = get(field);
      if (value instanceof ConfigOption) {
        add(options, (ConfigOption<?>) value);
      } else if (value instanceof Collection) {
        for (Object element : (Collection<?>) value) {
          if (element instanceof ConfigOption) {
            add(options, (ConfigOption<?>) element);
          }
        }
      }
    }
    return options;
  }

  /** The prefixes the catalog passes through: its public static String and String-list fields. */
  private static Set<String> passThroughPrefixes() {
    Set<String> prefixes = new LinkedHashSet<>();
    for (Field field : SchemaRegistryCatalogOptions.class.getFields()) {
      Object value = Modifier.isStatic(field.getModifiers()) ? get(field) : null;
      if (value instanceof String) {
        prefixes.add((String) value);
      } else if (value instanceof Collection) {
        for (Object element : (Collection<?>) value) {
          if (element instanceof String) {
            prefixes.add((String) element);
          }
        }
      }
    }
    return prefixes;
  }

  private static void add(Map<String, ConfigOption<?>> options, ConfigOption<?> option) {
    ConfigOption<?> previous = options.put(option.key(), option);
    if (previous != null && previous != option) {
      throw new IllegalStateException("two options share the key " + option.key());
    }
  }

  private static Object get(Field field) {
    try {
      return field.get(null);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static Map<String, ConfigOption<?>> options(ConfigOption<?>... options) {
    Map<String, ConfigOption<?>> map = new LinkedHashMap<>();
    for (ConfigOption<?> option : options) {
      map.put(option.key(), option);
    }
    return map;
  }

  private static Set<String> keys(Set<ConfigOption<?>> options) {
    return options.stream().map(ConfigOption::key).collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static String table(String header, String... rows) {
    return "Intro.\n\n" + header + "\n| --- |\n" + String.join("\n", rows) + "\n\nAfter.\n";
  }

  private static String read(String page) throws IOException {
    String configured = System.getProperty("docs.contentDir");
    Path dir = configured != null ? Paths.get(configured) : Paths.get("docs", "content");
    return Files.readString(dir.resolve(page), StandardCharsets.UTF_8);
  }

  private static String report(String page, List<String> problems) {
    return "docs/content/"
        + page
        + " does not match the options in the code; update the table:\n  "
        + String.join("\n  ", problems);
  }
}
