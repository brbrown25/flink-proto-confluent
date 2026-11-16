package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.ORDER_FULL_NAME;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.SUBJECT;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.decode;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.orderSchema;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.serializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoToRowDataConverters;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.math.BigDecimal;
import java.security.CodeSource;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * The format must work on the protobuf-java version the POM declares (issue #189). The build once
 * declared 4.31.1 while its own classpath silently resolved a newer one; a job that pinned the
 * declared version then failed with {@code NoClassDefFoundError: com/google/protobuf/GeneratedFile}
 * as soon as {@code ProtobufSchema} initialised, because classes generated into
 * proto-google-common-protos need a newer runtime.
 *
 * <p>The Gradle task {@code declaredProtobufTest} runs this class on a classpath where protobuf is
 * forced to exactly {@code protoVersion} and passes that version as {@code declaredProtoVersion}.
 * In the ordinary {@code test} task the round trips still run; only the check that the classpath is
 * the pinned one is skipped.
 */
class DeclaredProtobufRuntimeTest {

  private static final String DECLARED_VERSION = System.getProperty("declaredProtoVersion");

  /** File name of the JAR the class was loaded from. */
  private static String jarOf(Class<?> type) {
    CodeSource source = type.getProtectionDomain().getCodeSource();
    assertNotNull(source, () -> type + " has no code source");
    String location = source.getLocation().getPath();
    return location.substring(location.lastIndexOf('/') + 1);
  }

  @Test
  void classpathHoldsExactlyTheDeclaredProtobufVersion() {
    assumeTrue(DECLARED_VERSION != null, "only meaningful in the declaredProtobufTest task");

    assertEquals("protobuf-java-" + DECLARED_VERSION + ".jar", jarOf(Message.class));
    assertEquals("protobuf-java-util-" + DECLARED_VERSION + ".jar", jarOf(JsonFormat.class));
  }

  /** The sink's lookup path: parses a registry schema, which is what initialises ProtobufSchema. */
  @Test
  void existingSubject_roundTripsOnTheDeclaredRuntime() throws Exception {
    StubSchemaRegistryClient registry = new StubSchemaRegistryClient();
    registry.seed(SUBJECT, orderSchema());
    RowType rowType = rowType("content", "date_time");

    byte[] bytes =
        serializer(registry, false).serializeRowData(TOPIC, rowType, row("hello", "2026-01-01"));
    DynamicMessage decoded = decode(registry, bytes);
    RowData back =
        (RowData)
            ProtoToRowDataConverters.createConverter(decoded.getDescriptorForType(), rowType)
                .convert(decoded);

    assertEquals(ORDER_FULL_NAME, decoded.getDescriptorForType().getFullName());
    assertEquals("hello", back.getString(0).toString());
    assertEquals("2026-01-01", back.getString(1).toString());
  }

  /**
   * The sink's derive-and-register path with the column types that map onto generated classes from
   * other artifacts: {@code google.type.Date} and {@code google.type.TimeOfDay} from
   * proto-google-common-protos, {@code google.protobuf.Timestamp} from protobuf-java and Confluent's
   * {@code Decimal}. Each of those classes is initialised by its own generated file descriptor.
   */
  @Test
  void derivedSchemaWithWellKnownTypes_roundTripsOnTheDeclaredRuntime() throws Exception {
    StubSchemaRegistryClient registry = new StubSchemaRegistryClient();
    RowType rowType =
        RowType.of(
            new LogicalType[] {
              new VarCharType(VarCharType.MAX_LENGTH),
              new DateType(),
              new TimeType(3),
              new LocalZonedTimestampType(3),
              new DecimalType(10, 2)
            },
            new String[] {"name", "day", "at", "seen", "amount"});
    int day = (int) LocalDate.of(2026, 11, 16).toEpochDay();
    int at = LocalTime.of(13, 45, 30).toSecondOfDay() * 1000;
    TimestampData seen = TimestampData.fromInstant(Instant.parse("2026-11-16T13:45:30.123Z"));
    DecimalData amount = DecimalData.fromBigDecimal(new BigDecimal("1234.56"), 10, 2);
    GenericRowData row = GenericRowData.of(StringData.fromString("a"), day, at, seen, amount);

    byte[] bytes = serializer(registry, true).serializeRowData(TOPIC, rowType, row);
    DynamicMessage decoded = decode(registry, bytes);
    RowData back =
        (RowData)
            ProtoToRowDataConverters.createConverter(decoded.getDescriptorForType(), rowType)
                .convert(decoded);

    assertEquals("a", back.getString(0).toString());
    assertEquals(day, back.getInt(1));
    assertEquals(at, back.getInt(2));
    assertEquals(seen, back.getTimestamp(3, 3));
    assertEquals(amount, back.getDecimal(4, 10, 2));
  }
}
