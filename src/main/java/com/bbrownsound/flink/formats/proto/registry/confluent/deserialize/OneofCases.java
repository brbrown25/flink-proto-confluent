package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.RowType;

/**
 * Matches the fields of a {@code oneof} column (a {@code ROW} of the oneof's cases) to the oneof's
 * cases, the same way for a full read, a nested projection and the {@code message-class} check.
 *
 * <p>A field is matched to the case of its name, exact, then {@code snake_case} to {@code
 * camelCase}, as other columns are matched to fields (and as the sink matches them). A field that
 * names no case reads the case at its position in the {@code ROW}, as before cases were matched by
 * name, unless another field of the {@code ROW} names that case: then it, and any field past the
 * last case, has no case.
 */
final class OneofCases {

  /** The case of each field of the {@code ROW}, by field index; {@code null} where it has none. */
  final FieldDescriptor[] cases;

  /** Whether each field's case is the one at its position, because the field names no case. */
  final boolean[] byPosition;

  private OneofCases(FieldDescriptor[] cases, boolean[] byPosition) {
    this.cases = cases;
    this.byPosition = byPosition;
  }

  /**
   * Matches the fields of {@code rowType} to the cases of {@code oneof}.
   *
   * @param oneof the oneof the column reads
   * @param rowType the column's type
   * @return the case of each field of {@code rowType}
   */
  static OneofCases match(OneofDescriptor oneof, RowType rowType) {
    final Map<String, FieldDescriptor> byName =
        oneof.getFields().stream()
            .collect(Collectors.toMap(FieldDescriptor::getName, Function.identity()));
    final List<String> names = rowType.getFieldNames();
    final int count = names.size();
    final FieldDescriptor[] cases = new FieldDescriptor[count];
    final boolean[] byPosition = new boolean[count];
    final Set<FieldDescriptor> named = new HashSet<>();
    for (int i = 0; i < count; i++) {
      cases[i] = ProtoToRowDataConverters.findFieldDescriptor(byName, names.get(i));
      if (cases[i] != null) {
        named.add(cases[i]);
      }
    }
    for (int i = 0; i < count && i < oneof.getFieldCount(); i++) {
      if (cases[i] == null && !named.contains(oneof.getField(i))) {
        cases[i] = oneof.getField(i);
        byPosition[i] = true;
      }
    }
    return new OneofCases(cases, byPosition);
  }

  /**
   * Why a field of the {@code ROW} has no case, as a column check problem.
   *
   * @param oneof the oneof the column reads
   * @param columnPath the field's path, from the table's top-level column
   * @return the problem
   */
  static String noCase(OneofDescriptor oneof, String columnPath) {
    return "column '"
        + columnPath
        + "' has no case in oneof '"
        + oneof.getFullName()
        + "' (cases: "
        + oneof.getFields().stream().map(FieldDescriptor::getName).collect(Collectors.joining(", "))
        + "; a field that names no case reads the case at its position, unless another field"
        + " names that case)";
  }
}
