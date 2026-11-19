package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * One column of a nested projection, resolved against a message descriptor: the Protobuf fields a
 * reader walks from the top-level message to the value the column holds.
 *
 * <p>A projection path is a list of column indexes into the table's physical row type, one per
 * level of nesting (for {@code payload.inner.x}: the index of {@code payload}, of {@code inner}
 * within it, and of {@code x} within that). Each level is matched to the message the same way a
 * full read matches it: a column named after a {@code oneof} reads that oneof, whose own columns
 * are its cases by position, and any other column is the field of that name (exact, then {@code
 * snake_case} to {@code camelCase}). Only {@code ROW} columns can be descended into, so a message
 * column declared as {@code STRING} or {@code BYTES} is always read whole.
 */
final class ProjectedPath {

  /** The declared column names along the path, dot-separated, for messages. */
  final String columnPath;

  /**
   * The fields walked from the top-level message. The last one holds the column's value, unless
   * {@link #oneof} is set, in which case every field leads to the message holding that oneof.
   */
  final FieldDescriptor[] fields;

  /** The oneof the column reads (as a {@code ROW} of its cases), or {@code null}. */
  final OneofDescriptor oneof;

  /** The type of the projected column. */
  final LogicalType type;

  /** Why the path does not resolve, as a column check problem, or {@code null} if it does. */
  final String problem;

  /**
   * Whether {@link #problem} is a column the message does not have, which a reader reads as NULL
   * (as it does for a top-level column), rather than a column it cannot read.
   */
  final boolean missing;

  private ProjectedPath(
      String columnPath,
      List<FieldDescriptor> fields,
      OneofDescriptor oneof,
      LogicalType type,
      String problem,
      boolean missing) {
    this.columnPath = columnPath;
    this.fields = fields.toArray(new FieldDescriptor[0]);
    this.oneof = oneof;
    this.type = type;
    this.problem = problem;
    this.missing = missing;
  }

  /**
   * Resolves one projection path.
   *
   * @param message the top-level message
   * @param rowType the table's physical row type
   * @param path the column index at each level of nesting, as Flink's {@code Projection} holds it
   * @return the resolved path; check {@link #problem} before reading it
   */
  static ProjectedPath resolve(Descriptor message, RowType rowType, int[] path) {
    final List<FieldDescriptor> fields = new ArrayList<>(path.length);
    Descriptor descriptor = message;
    RowType row = rowType;
    String columnPath = "";
    int k = 0;
    while (true) {
      final RowType.RowField column = row.getFields().get(path[k]);
      columnPath = child(columnPath, column.getName());
      LogicalType type = column.getType();
      final OneofDescriptor oneof = oneofs(descriptor).get(column.getName());
      final FieldDescriptor field;
      if (oneof != null) {
        if (k == path.length - 1) {
          return new ProjectedPath(columnPath, fields, oneof, type, null, false);
        }
        // The oneof's columns are its cases by position.
        final RowType cases = (RowType) type;
        final int caseIndex = path[++k];
        columnPath = child(columnPath, cases.getFieldNames().get(caseIndex));
        type = cases.getTypeAt(caseIndex);
        if (caseIndex >= oneof.getFieldCount()) {
          return missing(
              columnPath,
              type,
              "has no case in oneof '"
                  + oneof.getFullName()
                  + "', which has "
                  + oneof.getFieldCount()
                  + " case(s)");
        }
        field = oneof.getField(caseIndex);
      } else {
        field = ProtoToRowDataConverters.findFieldDescriptor(fields(descriptor), column.getName());
        if (field == null) {
          return missing(
              columnPath,
              type,
              "has no field in message '"
                  + descriptor.getFullName()
                  + "' (fields: "
                  + String.join(", ", SourceMessageValidator.fieldNames(descriptor))
                  + ")");
        }
      }
      fields.add(field);
      if (k == path.length - 1) {
        return new ProjectedPath(columnPath, fields, null, type, null, false);
      }
      if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE || field.isRepeated()) {
        // Only a ROW column is descended into, and a ROW reads a single message field.
        final String problem =
            SourceMessageValidator.mismatch(
                columnPath,
                type,
                field,
                field.isRepeated() ? "an ARRAY or a MAP" : "a value, not a message");
        return new ProjectedPath(columnPath, fields, null, type, problem, false);
      }
      descriptor = field.getMessageType();
      row = (RowType) type;
      k++;
    }
  }

  private static ProjectedPath missing(String columnPath, LogicalType type, String reason) {
    return new ProjectedPath(
        columnPath, List.of(), null, type, "column '" + columnPath + "' " + reason, true);
  }

  private static Map<String, OneofDescriptor> oneofs(Descriptor descriptor) {
    return descriptor.getRealOneofs().stream()
        .collect(Collectors.toMap(OneofDescriptor::getName, Function.identity()));
  }

  /** The fields a column can name directly: fields inside a oneof are reached through it. */
  private static Map<String, FieldDescriptor> fields(Descriptor descriptor) {
    return descriptor.getFields().stream()
        .filter(f -> f.getRealContainingOneof() == null)
        .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));
  }

  private static String child(String path, String name) {
    return path.isEmpty() ? name : path + "." + name;
  }
}
