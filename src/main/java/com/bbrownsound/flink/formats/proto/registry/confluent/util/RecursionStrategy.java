package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import java.util.Locale;
import java.util.Objects;

/**
 * How derived DDL ({@link ProtoToSqlType}, and through it {@link ProtoToTableDDL}, the {@code ddl}
 * command and the Schema Registry catalog) maps a field that makes a message contain itself, such
 * as {@code child} in {@code message Tree { Tree child = 1; }}. Such a message has no finite {@code
 * ROW} type.
 *
 * <p>Written as the value of the {@code recursive-message} option (catalog) or {@code
 * --recursive-message} flag ({@code ddl} command):
 *
 * <ul>
 *   <li>{@code fail} (the default): derivation fails, naming the message and the field.
 *   <li>{@code bytes}: the field is {@code BYTES}, the message's Protobuf encoding.
 *   <li>{@code json}: the field is {@code STRING}, the message's JSON.
 *   <li>{@code row:<n>}: the field is a {@code ROW}, unrolled {@code n} times; the levels below are
 *       left out. A field inside a {@code oneof} cannot be left out (the oneof's columns are
 *       matched to its cases by position), so at the last level it is {@code BYTES} instead.
 * </ul>
 *
 * <p>Each type is one the converters read and write for the field (see {@link DeclaredTypes}), so
 * the derived table reads and writes the topic. {@code bytes} and {@code json} need a message
 * without a field named {@code value}: one with such a field is read through that field, like a
 * wrapper type, so derivation fails for it and suggests {@code row:<n>}.
 */
public final class RecursionStrategy {

  /** The name of the catalog option and, with {@code --}, of the {@code ddl} flag. */
  public static final String OPTION = "recursive-message";

  /** The ways a recursive field can be mapped. */
  public enum Mode {
    /** Fail derivation. */
    FAIL,
    /** Map the field to {@code BYTES}. */
    BYTES,
    /** Map the field to {@code STRING} (JSON). */
    JSON,
    /** Unroll the field to a fixed depth. */
    ROW
  }

  /** Fails on a recursive message: the default. */
  public static final RecursionStrategy FAIL = new RecursionStrategy(Mode.FAIL, 0);

  /** Maps a recursive field to {@code BYTES}. */
  public static final RecursionStrategy BYTES = new RecursionStrategy(Mode.BYTES, 0);

  /** Maps a recursive field to {@code STRING} holding the message's JSON. */
  public static final RecursionStrategy JSON = new RecursionStrategy(Mode.JSON, 0);

  /** The values {@link #parse} accepts, for error and help messages. */
  public static final String VALUES = "'fail', 'bytes', 'json' or 'row:<depth>'";

  private static final String ROW_PREFIX = "row:";

  private final Mode mode;
  private final int depth;

  private RecursionStrategy(Mode mode, int depth) {
    this.mode = mode;
    this.depth = depth;
  }

  /**
   * Returns the strategy that unrolls a recursive field {@code depth} times: with {@code depth} 2,
   * {@code Tree} becomes {@code ROW<name, child ROW<name, child ROW<name>>>}.
   *
   * @param depth how many times the recursive field is followed, at least 1
   * @return the strategy
   * @throws IllegalArgumentException if {@code depth} is less than 1
   */
  public static RecursionStrategy row(int depth) {
    if (depth < 1) {
      throw new IllegalArgumentException(
          "The depth of '" + OPTION + "' = 'row:<depth>' must be at least 1, got " + depth);
    }
    return new RecursionStrategy(Mode.ROW, depth);
  }

  /**
   * Parses an option value: {@code fail}, {@code bytes}, {@code json} or {@code row:<depth>},
   * ignoring case.
   *
   * @param value the option value
   * @return the strategy
   * @throws IllegalArgumentException if the value is none of those
   */
  public static RecursionStrategy parse(String value) {
    final String normalized = value.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case "fail":
        return FAIL;
      case "bytes":
        return BYTES;
      case "json":
        return JSON;
      default:
        break;
    }
    if (normalized.startsWith(ROW_PREFIX)) {
      try {
        return row(Integer.parseInt(normalized.substring(ROW_PREFIX.length()).trim()));
      } catch (NumberFormatException e) {
        // reported below
      }
    }
    throw new IllegalArgumentException(
        "'" + OPTION + "' must be " + VALUES + " (e.g. 'row:2'), got '" + value + "'");
  }

  /** Returns the mode. */
  public Mode getMode() {
    return mode;
  }

  /** Returns how many times {@link Mode#ROW} follows a recursive field (0 for other modes). */
  public int getDepth() {
    return depth;
  }

  /** Returns the option value this strategy is parsed from, e.g. {@code row:2}. */
  @Override
  public String toString() {
    return mode == Mode.ROW ? ROW_PREFIX + depth : mode.name().toLowerCase(Locale.ROOT);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof RecursionStrategy)) {
      return false;
    }
    final RecursionStrategy other = (RecursionStrategy) o;
    return mode == other.mode && depth == other.depth;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode, depth);
  }
}
