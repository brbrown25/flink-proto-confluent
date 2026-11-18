package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

/**
 * The format and the {@code ddl} tool share one connection-option mapping ({@link
 * SchemaRegistryClients}); the format's option list must cover exactly the shared mapping, so a
 * connection option added to one side and not the other fails here.
 */
class ConnectionOptionsTest {

  @Test
  void formatConnectionOptionsMatchTheSharedMapping() {
    Set<String> formatKeys =
        RegistryProtoFormatFactory.CONNECTION_OPTIONS.stream()
            .map(ConfigOption::key)
            .collect(Collectors.toSet());
    assertEquals(SchemaRegistryClients.CONNECTION_OPTIONS.keySet(), formatKeys);
    assertEquals(
        formatKeys.size(),
        new RegistryProtoFormatFactory()
            .optionalOptions().stream()
                .map(ConfigOption::key)
                .filter(formatKeys::contains)
            .count(),
        "every connection option is a declared format option");
  }

  @Test
  void formatBuildsTheSameClientPropertiesAsTheSharedMapping() {
    Map<String, String> byName = new HashMap<>();
    int i = 0;
    for (String option : SchemaRegistryClients.CONNECTION_OPTIONS.keySet()) {
      byName.put(option, "value-" + i++);
    }
    byName.put("properties.max.retries", "3");
    Configuration formatOptions = Configuration.fromMap(byName);

    assertEquals(
        SchemaRegistryClients.clientProperties(byName),
        RegistryProtoFormatFactory.buildOptionalPropertiesMap(formatOptions));
    assertNull(RegistryProtoFormatFactory.buildOptionalPropertiesMap(new Configuration()));
  }
}
