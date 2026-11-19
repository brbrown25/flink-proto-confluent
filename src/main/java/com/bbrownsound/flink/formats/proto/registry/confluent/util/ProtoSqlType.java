package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The Flink SQL type of a protobuf field, as a small tree that does not depend on Flink.
 *
 * <p>{@link ProtoToSqlType} builds it from a protobuf descriptor; it is the single place that
 * decides which SQL type a protobuf field maps to. {@link ProtoToLogicalType} turns it into Flink's
 * {@code LogicalType} for code running inside Flink, and {@link #toDdl()} renders it as Flink SQL
 * DDL for code that must run without Flink on the classpath, such as the {@code ddl} command line
 * tool shipped in the shadow JAR.
 */
public final class ProtoSqlType {

  /** The SQL types a protobuf schema maps to. */
  public enum Kind {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    STRING,
    BYTES,
    DATE,
    TIME,
    TIMESTAMP_LTZ,
    ROW,
    ARRAY,
    MAP
  }

  /** A named field of a {@link Kind#ROW} type. */
  public static final class Field {
    private final String name;
    private final ProtoSqlType type;

    /**
     * Creates a field.
     *
     * @param name the field name
     * @param type the field type
     */
    public Field(String name, ProtoSqlType type) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
    }

    /** Returns the field name. */
    public String getName() {
      return name;
    }

    /** Returns the field type. */
    public ProtoSqlType getType() {
      return type;
    }
  }

  private final Kind kind;
  private final boolean nullable;
  private final int precision;
  private final int scale;
  private final List<Field> fields;
  private final ProtoSqlType elementType;
  private final ProtoSqlType keyType;
  private final ProtoSqlType valueType;

  private ProtoSqlType(
      Kind kind,
      boolean nullable,
      int precision,
      int scale,
      List<Field> fields,
      ProtoSqlType elementType,
      ProtoSqlType keyType,
      ProtoSqlType valueType) {
    this.kind = kind;
    this.nullable = nullable;
    this.precision = precision;
    this.scale = scale;
    this.fields = fields;
    this.elementType = elementType;
    this.keyType = keyType;
    this.valueType = valueType;
  }

  /**
   * A type without parameters or children: every kind except DECIMAL, TIME, TIMESTAMP_LTZ, ROW,
   * ARRAY and MAP.
   *
   * @param kind the kind
   * @param nullable whether the type is nullable
   * @return the type
   */
  public static ProtoSqlType of(Kind kind, boolean nullable) {
    switch (kind) {
      case DECIMAL:
      case TIME:
      case TIMESTAMP_LTZ:
      case ROW:
      case ARRAY:
      case MAP:
        throw new IllegalArgumentException(kind + " needs parameters; use its own factory");
      default:
        return new ProtoSqlType(kind, nullable, 0, 0, null, null, null, null);
    }
  }

  /** Returns {@code DECIMAL(precision, scale)}. */
  public static ProtoSqlType decimal(boolean nullable, int precision, int scale) {
    return new ProtoSqlType(Kind.DECIMAL, nullable, precision, scale, null, null, null, null);
  }

  /** Returns {@code TIME(precision)}. */
  public static ProtoSqlType time(boolean nullable, int precision) {
    return new ProtoSqlType(Kind.TIME, nullable, precision, 0, null, null, null, null);
  }

  /** Returns {@code TIMESTAMP_LTZ(precision)}. */
  public static ProtoSqlType timestampLtz(boolean nullable, int precision) {
    return new ProtoSqlType(Kind.TIMESTAMP_LTZ, nullable, precision, 0, null, null, null, null);
  }

  /** Returns {@code ROW<fields>}. */
  public static ProtoSqlType row(boolean nullable, List<Field> fields) {
    return new ProtoSqlType(
        Kind.ROW,
        nullable,
        0,
        0,
        Collections.unmodifiableList(new ArrayList<>(fields)),
        null,
        null,
        null);
  }

  /** Returns {@code ARRAY<elementType>}. */
  public static ProtoSqlType array(boolean nullable, ProtoSqlType elementType) {
    return new ProtoSqlType(
        Kind.ARRAY, nullable, 0, 0, null, Objects.requireNonNull(elementType), null, null);
  }

  /** Returns {@code MAP<keyType, valueType>}. */
  public static ProtoSqlType map(boolean nullable, ProtoSqlType keyType, ProtoSqlType valueType) {
    return new ProtoSqlType(
        Kind.MAP,
        nullable,
        0,
        0,
        null,
        null,
        Objects.requireNonNull(keyType),
        Objects.requireNonNull(valueType));
  }

  /** Returns the kind. */
  public Kind getKind() {
    return kind;
  }

  /** Returns whether the type is nullable. */
  public boolean isNullable() {
    return nullable;
  }

  /** Returns the precision of DECIMAL, TIME and TIMESTAMP_LTZ; 0 for other kinds. */
  public int getPrecision() {
    return precision;
  }

  /** Returns the scale of DECIMAL; 0 for other kinds. */
  public int getScale() {
    return scale;
  }

  /** Returns the fields of a ROW; empty for other kinds. */
  public List<Field> getFields() {
    return fields == null ? Collections.emptyList() : fields;
  }

  /** Returns the element type of an ARRAY, or {@code null}. */
  public ProtoSqlType getElementType() {
    return elementType;
  }

  /** Returns the key type of a MAP, or {@code null}. */
  public ProtoSqlType getKeyType() {
    return keyType;
  }

  /** Returns the value type of a MAP, or {@code null}. */
  public ProtoSqlType getValueType() {
    return valueType;
  }

  /**
   * Returns this type with the given nullability; children are unchanged.
   *
   * @param isNullable the nullability of the copy
   * @return the copy
   */
  public ProtoSqlType withNullable(boolean isNullable) {
    return new ProtoSqlType(
        kind, isNullable, precision, scale, fields, elementType, keyType, valueType);
  }

  /**
   * Returns a copy with "strict NOT NULL" applied, the same transformation as {@link
   * LogicalTypeToDDL#toStrictNotNull(org.apache.flink.table.types.logical.LogicalType, String,
   * Set)}: TIMESTAMP_LTZ, DATE and TIME stay nullable, ROW nullability is preserved, array and map
   * containers and ROW array elements become NOT NULL, and every other type becomes NOT NULL unless
   * its path is in {@code keepNullablePaths}. Paths are dot-separated field names; array elements
   * add {@code []}, map keys and values add {@code .key} and {@code .value}.
   *
   * @param keepNullablePaths paths that stay nullable, or {@code null}
   * @return the transformed copy
   */
  public ProtoSqlType toStrictNotNull(Set<String> keepNullablePaths) {
    return toStrictNotNull(this, "", keepNullablePaths);
  }

  private static ProtoSqlType toStrictNotNull(
      ProtoSqlType type, String path, Set<String> keepNullablePaths) {
    switch (type.kind) {
      case TIMESTAMP_LTZ:
      case DATE:
      case TIME:
        return type.withNullable(true);
      case ROW:
        List<Field> newFields = new ArrayList<>(type.fields.size());
        for (Field field : type.fields) {
          String childPath = path.isEmpty() ? field.name : path + "." + field.name;
          newFields.add(
              new Field(field.name, toStrictNotNull(field.type, childPath, keepNullablePaths)));
        }
        return row(type.nullable, newFields);
      case ARRAY:
        ProtoSqlType element = toStrictNotNull(type.elementType, path + "[]", keepNullablePaths);
        if (element.kind == Kind.ROW && element.nullable) {
          element = element.withNullable(false);
        }
        return array(false, element);
      case MAP:
        return map(
            false,
            toStrictNotNull(type.keyType, path + ".key", keepNullablePaths),
            toStrictNotNull(type.valueType, path + ".value", keepNullablePaths));
      default:
        return type.withNullable(keepNullablePaths != null && keepNullablePaths.contains(path));
    }
  }

  /**
   * Returns the Flink SQL type string, e.g. {@code STRING NOT NULL}, {@code ROW<`a` INT>} or
   * {@code TIMESTAMP_LTZ(9)}. The output is identical to {@link
   * LogicalTypeToDDL#toDDLTypeString(org.apache.flink.table.types.logical.LogicalType)} for the
   * corresponding Flink type.
   *
   * @return the type string, with {@code NOT NULL} when the type is not nullable
   */
  public String toDdl() {
    StringBuilder sb = new StringBuilder();
    appendDdl(sb);
    return sb.toString();
  }

  /**
   * Returns the column definitions of a ROW type, one per field, e.g. {@code `id` STRING}.
   *
   * @return the column definitions in field order
   * @throws IllegalStateException if this is not a ROW type
   */
  public List<String> toColumnDefinitions() {
    if (kind != Kind.ROW) {
      throw new IllegalStateException("Column definitions need a ROW type, got: " + toDdl());
    }
    List<String> columns = new ArrayList<>(fields.size());
    for (Field field : fields) {
      columns.add(quoteIdentifier(field.name) + " " + field.type.toDdl());
    }
    return columns;
  }

  /**
   * Quotes a Flink SQL identifier with backticks, doubling any backtick inside it.
   *
   * @param identifier the identifier
   * @return the quoted identifier
   */
  public static String quoteIdentifier(String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  private void appendDdl(StringBuilder sb) {
    switch (kind) {
      case INT:
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BYTES:
      case DATE:
        // BYTES is VARBINARY(2147483647); a bare VARBINARY would mean VARBINARY(1) (issue #222).
        sb.append(kind.name());
        break;
      case DECIMAL:
        sb.append("DECIMAL(").append(precision).append(",").append(scale).append(")");
        break;
      case TIME:
        sb.append("TIME(").append(precision).append(")");
        break;
      case TIMESTAMP_LTZ:
        sb.append("TIMESTAMP_LTZ(").append(precision).append(")");
        break;
      case ROW:
        sb.append("ROW<").append(String.join(", ", toColumnDefinitions())).append(">");
        break;
      case ARRAY:
        sb.append("ARRAY<").append(elementType.toDdl()).append(">");
        break;
      default: // MAP, the last kind
        sb.append("MAP<").append(keyType.toDdl()).append(", ").append(valueType.toDdl());
        sb.append(">");
        break;
    }
    if (!nullable) {
      sb.append(" NOT NULL");
    }
  }

  @Override
  public String toString() {
    return toDdl();
  }
}
