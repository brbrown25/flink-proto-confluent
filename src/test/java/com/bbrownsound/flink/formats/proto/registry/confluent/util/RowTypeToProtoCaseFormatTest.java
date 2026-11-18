package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.base.CaseFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The map entry type name used to come from Flink's shaded Guava {@code CaseFormat}. The helper
 * that replaced it must produce exactly the same names, or the generated schema for existing map
 * columns would change.
 */
class RowTypeToProtoCaseFormatTest {

  private static final List<String> INPUTS =
      List.of(
          "tags_Entry",
          "my_map_Entry",
          "myMap_Entry",
          "MY_MAP_ENTRY",
          "a",
          "_leading_Entry",
          "trailing__Entry",
          "double__underscore",
          "x1_y2_Entry",
          "already_UPPER_Entry",
          "ümlaut_Entry",
          "",
          "_");

  @Test
  void matchesGuavaLowerUnderscoreToUpperCamel() {
    for (String input : INPUTS) {
      assertEquals(
          CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, input),
          RowTypeToProto.lowerUnderscoreToUpperCamel(input),
          "input: '" + input + "'");
    }
  }

  @Test
  void mapEntryNameForTypicalField() {
    assertEquals("TagsEntry", RowTypeToProto.lowerUnderscoreToUpperCamel("tags_Entry"));
    assertEquals("MyMapEntry", RowTypeToProto.lowerUnderscoreToUpperCamel("my_map_Entry"));
  }
}
