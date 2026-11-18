package com.bbrownsound.flink.formats.proto.registry.confluent.subject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class FixedSubjectNameStrategyTest {

  @Test
  void returnsTheConfiguredSubjectPerRole_ignoringTopicAndSchema() {
    FixedSubjectNameStrategy strategy = new FixedSubjectNameStrategy();
    strategy.configure(
        Map.of(
            FixedSubjectNameStrategy.KEY_SUBJECT_CONFIG, "orders-key-custom",
            FixedSubjectNameStrategy.VALUE_SUBJECT_CONFIG, " orders-custom "));
    assertEquals("orders-key-custom", strategy.subjectName("orders", true, null));
    assertEquals("orders-custom", strategy.subjectName("orders", false, null));
    assertEquals("orders-custom", strategy.subjectName(null, false, null));
    assertFalse(strategy.usesSchema());
  }

  @Test
  void missingSubjectForTheRole_failsNamingTheConfigKey() {
    FixedSubjectNameStrategy strategy = new FixedSubjectNameStrategy();
    strategy.configure(
        Map.of(
            FixedSubjectNameStrategy.VALUE_SUBJECT_CONFIG, "orders-custom",
            FixedSubjectNameStrategy.KEY_SUBJECT_CONFIG, ""));
    ConfigException e =
        assertThrows(ConfigException.class, () -> strategy.subjectName("orders", true, null));
    assertTrue(e.getMessage().contains("'key.subject' is not set"), e.getMessage());

    FixedSubjectNameStrategy unconfigured = new FixedSubjectNameStrategy();
    unconfigured.configure(Map.of());
    e = assertThrows(ConfigException.class, () -> unconfigured.subjectName("t", false, null));
    assertTrue(e.getMessage().contains("'value.subject' is not set"), e.getMessage());
  }

  @Test
  void strategyAliases_mapToConfluentClasses() {
    assertEquals(
        "io.confluent.kafka.serializers.subject.TopicNameStrategy",
        SubjectNameStrategies.className(" Topic "));
    assertEquals("com.example.Custom", SubjectNameStrategies.className("com.example.Custom"));
    assertEquals("key.subject.name.strategy", SubjectNameStrategies.strategyConfigKey(true));
    assertEquals("value.subject.name.strategy", SubjectNameStrategies.strategyConfigKey(false));
  }
}
