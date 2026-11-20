package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.Type;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Column types a source reads a field into in place of the type derived for it, because the column
 * holds every value the field can hold. A field widened between schema versions ({@code int32} to
 * {@code int64}, {@code float} to {@code double}) can then be read from records of either version
 * by a table declared for the wider one.
 *
 * <ul>
 *   <li>{@code int32}, {@code sint32}, {@code sfixed32} and {@code google.protobuf.Int32Value} as
 *       {@code BIGINT}.
 *   <li>{@code float} and {@code google.protobuf.FloatValue} as {@code DOUBLE}: every {@code float}
 *       is exactly a {@code double}.
 *   <li>A 32-bit integer ({@code int32}, {@code sint32}, {@code sfixed32}, {@code uint32}, {@code
 *       fixed32}, {@code google.protobuf.Int32Value}, {@code UInt32Value}) as a {@code DECIMAL(p,
 *       s)} with {@code p - s >= 10}; unsigned fields read as their unsigned value.
 *   <li>A signed 64-bit integer ({@code int64}, {@code sint64}, {@code sfixed64}, {@code
 *       google.protobuf.Int64Value}) as a {@code DECIMAL(p, s)} with {@code p - s >= 19}.
 * </ul>
 *
 * <p>Read only: the sink writes these columns as it always has. A narrower column (an {@code INT}
 * for an {@code int64}, a {@code DECIMAL} with too few integer digits) is still rejected.
 */
final class WideningReads {

  /** Digits of the largest 32-bit magnitude, signed (2147483648) or unsigned (4294967295). */
  private static final int DIGITS_32 = 10;

  /** Digits of the largest signed 64-bit magnitude, 9223372036854775808. */
  private static final int DIGITS_64 = 19;

  private static final String INT32_WRAPPER = "google.protobuf.Int32Value";
  private static final String UINT32_WRAPPER = "google.protobuf.UInt32Value";
  private static final String INT64_WRAPPER = "google.protobuf.Int64Value";
  private static final String FLOAT_WRAPPER = "google.protobuf.FloatValue";

  private WideningReads() {}

  /**
   * Returns whether the source reads one value of the field (an element, for a repeated field) into
   * a column of the given type as a lossless widening listed on this class.
   */
  static boolean isWidening(FieldDescriptor field, LogicalType type) {
    switch (type.getTypeRoot()) {
      case BIGINT:
        return isSigned32(field);
      case DOUBLE:
        return field.getType() == Type.FLOAT || isMessage(field, FLOAT_WRAPPER);
      case DECIMAL:
        final int digits = integerDigits(field);
        return digits > 0 && integerDigits((DecimalType) type) >= digits;
      default:
        return false;
    }
  }

  /** Returns whether the field is a {@code uint32}, {@code fixed32} or {@code UInt32Value}. */
  static boolean isUnsigned32(FieldDescriptor field) {
    return field.getType() == Type.UINT32
        || field.getType() == Type.FIXED32
        || isMessage(field, UINT32_WRAPPER);
  }

  /**
   * Returns why a {@code DECIMAL} column cannot hold every value of an integer field, or {@code
   * null} when the field is not one {@link #isWidening} reads into a {@code DECIMAL}.
   */
  static String tooNarrowDecimal(FieldDescriptor field) {
    final int digits = integerDigits(field);
    if (digits == 0) {
      return null;
    }
    return "a DECIMAL needs at least "
        + digits
        + " digits before the decimal point (precision - scale >= "
        + digits
        + ") to hold every value";
  }

  private static int integerDigits(FieldDescriptor field) {
    if (isSigned32(field) || isUnsigned32(field)) {
      return DIGITS_32;
    }
    if (field.getType() == Type.INT64
        || field.getType() == Type.SINT64
        || field.getType() == Type.SFIXED64
        || isMessage(field, INT64_WRAPPER)) {
      return DIGITS_64;
    }
    return 0;
  }

  private static int integerDigits(DecimalType type) {
    return type.getPrecision() - type.getScale();
  }

  private static boolean isSigned32(FieldDescriptor field) {
    return field.getType() == Type.INT32
        || field.getType() == Type.SINT32
        || field.getType() == Type.SFIXED32
        || isMessage(field, INT32_WRAPPER);
  }

  private static boolean isMessage(FieldDescriptor field, String fullName) {
    return field.getType() == Type.MESSAGE
        && field.getMessageType().getFullName().equals(fullName);
  }
}
