package com.bbrownsound.flink.formats.proto.registry.confluent.config;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import java.util.Locale;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Covers GitHub issue #170: the {@code on-deserialize-error} enum and its parsing helpers. */
@DisplayName("OnDeserializeError")
class OnDeserializeErrorTest {

  @Test
  void values_areExactlyFailAndSkip() {
    assertArrayEquals(
        new OnDeserializeError[] {OnDeserializeError.FAIL, OnDeserializeError.SKIP},
        OnDeserializeError.values());
  }

  @Test
  void toString_isTheLowerCaseOptionValue() {
    assertEquals("fail", OnDeserializeError.FAIL.toString());
    assertEquals("skip", OnDeserializeError.SKIP.toString());
    assertEquals("fail, skip", OnDeserializeError.allowedValues());
  }

  @ParameterizedTest(name = "[{index}] ''{0}'' -> {1}")
  @CsvSource({
    "fail,FAIL", "FAIL,FAIL", "Fail,FAIL", "fAIL,FAIL",
    "skip,SKIP", "SKIP,SKIP", "Skip,SKIP", "sKIP,SKIP"
  })
  void fromString_isCaseInsensitive(String value, OnDeserializeError expected) {
    assertSame(expected, OnDeserializeError.fromString(value));
  }

  @ParameterizedTest(name = "[{index}] ''{0}''")
  @ValueSource(strings = {"fial", "", "skipp", " skip", "skip ", "bogus", "true", "fail,skip"})
  void fromString_unknownValue_throwsValidationExceptionListingAllowedValues(String value) {
    ValidationException e =
        assertThrows(ValidationException.class, () -> OnDeserializeError.fromString(value));
    assertTrue(e.getMessage().contains("'" + value + "'"), e.getMessage());
    assertTrue(e.getMessage().contains("'on-deserialize-error'"), e.getMessage());
    assertTrue(e.getMessage().contains("fail, skip"), e.getMessage());
    assertNull(e.getCause());
  }

  @Test
  void fromString_null_throwsValidationException() {
    ValidationException e =
        assertThrows(ValidationException.class, () -> OnDeserializeError.fromString(null));
    assertTrue(e.getMessage().contains("fail, skip"), e.getMessage());
  }

  @Test
  void fromString_isIndependentOfTheDefaultLocale() {
    // In a Turkish locale "skip".toUpperCase() is "SKİP" and "FAIL".toLowerCase() is "faıl".
    Locale previous = Locale.getDefault();
    Locale.setDefault(Locale.forLanguageTag("tr-TR"));
    try {
      assertSame(OnDeserializeError.SKIP, OnDeserializeError.fromString("skip"));
      assertSame(OnDeserializeError.FAIL, OnDeserializeError.fromString("FAIL"));
      assertEquals("fail", OnDeserializeError.FAIL.toString());
      assertEquals("skip", OnDeserializeError.SKIP.toString());
    } finally {
      Locale.setDefault(previous);
    }
  }

  @Test
  void fromOptions_unset_isFail() {
    assertSame(OnDeserializeError.FAIL, OnDeserializeError.fromOptions(new Configuration()));
  }

  @ParameterizedTest(name = "[{index}] {0} = ''{1}'' -> {2}")
  @CsvSource({
    "on-deserialize-error,fail,FAIL",
    "on-deserialize-error,FAIL,FAIL",
    "on-deserialize-error,Fail,FAIL",
    "on-deserialize-error,skip,SKIP",
    "on-deserialize-error,SKIP,SKIP",
    "on-deserialize-error,Skip,SKIP",
    "on_deserialize_error,fail,FAIL",
    "on_deserialize_error,FAIL,FAIL",
    "on_deserialize_error,Fail,FAIL",
    "on_deserialize_error,skip,SKIP",
    "on_deserialize_error,SKIP,SKIP",
    "on_deserialize_error,Skip,SKIP"
  })
  void fromOptions_acceptsBothKeysCaseInsensitively(
      String key, String value, OnDeserializeError expected) {
    assertSame(expected, OnDeserializeError.fromOptions(Configuration.fromMap(Map.of(key, value))));
  }

  @ParameterizedTest(name = "[{index}] {0} = ''{1}''")
  @CsvSource({
    "on-deserialize-error,fial",
    "on-deserialize-error,''",
    "on-deserialize-error,skipp",
    "on_deserialize_error,fial",
    "on_deserialize_error,''",
    "on_deserialize_error,skipp"
  })
  void fromOptions_unknownValue_throwsValidationExceptionListingAllowedValues(
      String key, String value) {
    Configuration options = Configuration.fromMap(Map.of(key, value));
    ValidationException e =
        assertThrows(ValidationException.class, () -> OnDeserializeError.fromOptions(options));
    assertEquals(
        "Invalid value '"
            + value
            + "' for option 'on-deserialize-error'. Allowed values (case-insensitive): fail, skip.",
        e.getMessage());
    assertNotNull(e.getCause(), "Flink's parse failure is kept as the cause");
  }

  @Test
  void fromOptions_typedValue_isReturned() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.ON_DESERIALIZE_ERROR, OnDeserializeError.SKIP);
    assertSame(OnDeserializeError.SKIP, OnDeserializeError.fromOptions(options));
  }

  @Test
  void survivesJavaSerialization() throws Exception {
    for (OnDeserializeError value : OnDeserializeError.values()) {
      assertSame(value, InstantiationUtil.clone(value));
    }
  }
}
