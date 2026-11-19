package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.RecursionStrategy.Mode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Parsing and printing the {@code recursive-message} option (issue #216). */
class RecursionStrategyTest {

  @Test
  void parsesEachMode() {
    assertSame(RecursionStrategy.FAIL, RecursionStrategy.parse("fail"));
    assertSame(RecursionStrategy.BYTES, RecursionStrategy.parse("bytes"));
    assertSame(RecursionStrategy.JSON, RecursionStrategy.parse(" JSON "));
    RecursionStrategy row = RecursionStrategy.parse("Row: 3");
    assertEquals(Mode.ROW, row.getMode());
    assertEquals(3, row.getDepth());
    assertEquals(RecursionStrategy.row(3), row);
  }

  @Test
  void printsTheOptionValue() {
    assertEquals("fail", RecursionStrategy.FAIL.toString());
    assertEquals("bytes", RecursionStrategy.BYTES.toString());
    assertEquals("json", RecursionStrategy.JSON.toString());
    assertEquals("row:2", RecursionStrategy.row(2).toString());
    assertEquals(Mode.BYTES, RecursionStrategy.BYTES.getMode());
    assertEquals(0, RecursionStrategy.BYTES.getDepth());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "row", "row:", "row:x", "row:-1", "depth:2", "string"})
  void rejectsOtherValues(String value) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> RecursionStrategy.parse(value));
    assertTrue(e.getMessage().contains("'recursive-message'"), e.getMessage());
  }

  @Test
  void rejectsADepthBelowOne() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> RecursionStrategy.parse("row:0"));
    assertTrue(e.getMessage().contains("at least 1, got 0"), e.getMessage());
  }

  @Test
  void equalsAndHashCode() {
    assertEquals(RecursionStrategy.row(2), RecursionStrategy.row(2));
    assertEquals(RecursionStrategy.row(2).hashCode(), RecursionStrategy.row(2).hashCode());
    assertNotEquals(RecursionStrategy.row(2), RecursionStrategy.row(3));
    assertNotEquals(RecursionStrategy.BYTES, RecursionStrategy.JSON);
    assertNotEquals(RecursionStrategy.BYTES, "bytes");
  }
}
