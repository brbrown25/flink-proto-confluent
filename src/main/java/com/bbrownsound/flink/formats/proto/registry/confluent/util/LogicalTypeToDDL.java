package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.TimeType;

/**
 * Converts a Flink {@link LogicalType} (typically a {@link RowType} from {@link
 * ProtoToLogicalType}) to a Flink SQL table DDL column list string. Field names are quoted with
 * backticks; types use NOT NULL where the type is not nullable. Useful for generating or validating
 * CREATE TABLE (...) schema from a protobuf message class.
 */
public final class LogicalTypeToDDL {

  /**
   * Returns a copy of the type tree with "strict NOT NULL" applied: TIMESTAMP_LTZ, DATE, and TIME
   * remain nullable; ROW nullability is preserved; all other scalars become NOT NULL. In proto3
   * singular fields are optional (nullable), so to match hand-written DDL that leaves only specific
   * fields nullable, use {@link #toStrictNotNull(LogicalType, String, Set)} with {@code
   * keepNullablePaths}.
   *
   * @param type the logical type to transform
   * @return a copy with strict NOT NULL applied
   */
  public static LogicalType toStrictNotNull(LogicalType type) {
    return toStrictNotNull(type, "", null);
  }

  /**
   * Like {@link #toStrictNotNull(LogicalType)} but also keeps nullable any field whose path is in
   * {@code keepNullablePaths}. Path is dot-separated (e.g. "first_seen",
   * "evidence.evidence.service.cloud_provider"); array elements use "[]". Use null or empty set
   * when proto nullability is sufficient.
   *
   * @param type the logical type to transform
   * @param path dot-separated path for the current type (used for keepNullablePaths matching)
   * @param keepNullablePaths set of paths that should remain nullable, or null
   * @return a copy with strict NOT NULL applied, except for paths in keepNullablePaths
   */
  public static LogicalType toStrictNotNull(
      LogicalType type, String path, Set<String> keepNullablePaths) {
    LogicalTypeRoot root = type.getTypeRoot();
    switch (root) {
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
        return type.copy(true);
      case ROW:
        RowType rowType = (RowType) type;
        List<RowField> newFields = new ArrayList<>(rowType.getFieldCount());
        for (RowField field : rowType.getFields()) {
          String childPath = path.isEmpty() ? field.getName() : path + "." + field.getName();
          newFields.add(
              new RowField(
                  field.getName(), toStrictNotNull(field.getType(), childPath, keepNullablePaths)));
        }
        return new RowType(rowType.isNullable(), newFields);
      case ARRAY:
        ArrayType arrayType = (ArrayType) type;
        LogicalType elementType =
            toStrictNotNull(arrayType.getElementType(), path + "[]", keepNullablePaths);
        if (elementType.getTypeRoot() == LogicalTypeRoot.ROW && elementType.isNullable()) {
          elementType = elementType.copy(false);
        }
        return new ArrayType(false, elementType);
      case MAP:
        MapType mapType = (MapType) type;
        return new MapType(
            false,
            toStrictNotNull(mapType.getKeyType(), path + ".key", keepNullablePaths),
            toStrictNotNull(mapType.getValueType(), path + ".value", keepNullablePaths));
      default:
        // Proto3 singular fields are optional (nullable). Hand-written DDL uses NOT NULL
        // for most scalars and leaves nullable only specific paths; pass keepNullablePaths.
        boolean keepNullable = keepNullablePaths != null && keepNullablePaths.contains(path);
        return type.copy(keepNullable);
    }
  }

  /**
   * Returns the DDL schema string for the given root type. For a RowType this is the
   * comma-separated list of column definitions suitable for use inside CREATE TABLE ( col1 type1,
   * col2 type2, ... ).
   *
   * @param rootType the root logical type (must be RowType)
   * @return comma-separated column DDL string
   */
  public static String toTableSchemaDDL(LogicalType rootType) {
    if (!(rootType instanceof RowType)) {
      throw new IllegalArgumentException("Root type must be RowType, got: " + rootType);
    }
    RowType rowType = (RowType) rootType;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      RowField field = rowType.getFields().get(i);
      sb.append("`").append(field.getName()).append("` ");
      sb.append(toDDLTypeString(field.getType()));
    }
    return sb.toString();
  }

  /**
   * Returns the DDL type string for a single type (e.g. STRING NOT NULL, ROW&lt;...&gt;,
   * TIMESTAMP_LTZ(9)).
   *
   * @param type the logical type
   * @return the DDL type string with NOT NULL if applicable
   */
  public static String toDDLTypeString(LogicalType type) {
    StringBuilder sb = new StringBuilder();
    appendType(sb, type);
    if (!type.isNullable()) {
      sb.append(" NOT NULL");
    }
    return sb.toString();
  }

  private static void appendType(StringBuilder sb, LogicalType type) {
    LogicalTypeRoot root = type.getTypeRoot();
    switch (root) {
      case VARCHAR:
      case CHAR:
        sb.append("STRING");
        break;
      case VARBINARY:
      case BINARY:
        sb.append("VARBINARY");
        break;
      case BOOLEAN:
        sb.append("BOOLEAN");
        break;
      case TINYINT:
        sb.append("TINYINT");
        break;
      case SMALLINT:
        sb.append("SMALLINT");
        break;
      case INTEGER:
        sb.append("INT");
        break;
      case BIGINT:
        sb.append("BIGINT");
        break;
      case FLOAT:
        sb.append("FLOAT");
        break;
      case DOUBLE:
        sb.append("DOUBLE");
        break;
      case DECIMAL:
        DecimalType decimalType = (DecimalType) type;
        sb.append("DECIMAL(")
            .append(decimalType.getPrecision())
            .append(",")
            .append(decimalType.getScale())
            .append(")");
        break;
      case DATE:
        sb.append("DATE");
        break;
      case TIME_WITHOUT_TIME_ZONE:
        TimeType timeType = (TimeType) type;
        sb.append("TIME(").append(timeType.getPrecision()).append(")");
        break;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        LocalZonedTimestampType ltzType = (LocalZonedTimestampType) type;
        sb.append("TIMESTAMP_LTZ(").append(ltzType.getPrecision()).append(")");
        break;
      case ROW:
        RowType rowType = (RowType) type;
        sb.append("ROW<");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
          if (i > 0) {
            sb.append(", ");
          }
          RowField field = rowType.getFields().get(i);
          sb.append("`").append(field.getName()).append("` ");
          sb.append(toDDLTypeString(field.getType()));
        }
        sb.append(">");
        break;
      case ARRAY:
        ArrayType arrayType = (ArrayType) type;
        sb.append("ARRAY<");
        sb.append(toDDLTypeString(arrayType.getElementType()));
        sb.append(">");
        break;
      case MAP:
        MapType mapType = (MapType) type;
        sb.append("MAP<");
        sb.append(toDDLTypeString(mapType.getKeyType()));
        sb.append(", ");
        sb.append(toDDLTypeString(mapType.getValueType()));
        sb.append(">");
        break;
      default:
        throw new IllegalArgumentException("Unsupported type for DDL: " + type);
    }
  }

  private LogicalTypeToDDL() {}
}
