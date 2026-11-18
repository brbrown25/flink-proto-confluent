package com.bbrownsound.flink.formats.proto.registry.confluent.bench;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.InMemoryDeserializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoRowDataDeserializationSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Source side: {@link ProtoRowDataDeserializationSchema#deserialize(byte[])} on one Confluent
 * wire-format record, from the bytes a Kafka consumer hands the format to the {@link RowData} the
 * table sees. The writer schema is in an in-memory registry and its converter is built during
 * setup, so every measured call is the steady-state per-record path.
 *
 * <p>Each benchmark runs for every {@link BenchMessage} and every {@link ReadShape}. To measure a
 * different way of reading the same record (for example a projected read), add a {@link ReadShape}
 * constant and its name to the {@code shape} parameter; nothing else changes.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class DeserializeBenchmark {

  /** How many leading top-level columns a {@link ReadShape#PROJECTED} read keeps. */
  static final int PROJECTED_COLUMNS = 2;

  /** How the table reads the record: which row type and schema the source is built with. */
  public enum ReadShape {
    /** Every field of the message is a column. */
    FULL,
    /**
     * The query reads only the first {@value DeserializeBenchmark#PROJECTED_COLUMNS} top-level
     * columns, and the planner pushed that projection into the format: the schema is built for the
     * projected row type, as {@code ProtoDecodingFormat} builds it (GitHub issue #173). Messages
     * with no more columns than that read the same as {@link #FULL}.
     */
    PROJECTED {
      @Override
      public RowType rowType(BenchMessage message) {
        List<RowType.RowField> fields = message.rowType().getFields();
        return new RowType(fields.subList(0, Math.min(PROJECTED_COLUMNS, fields.size())));
      }
    };

    /**
     * The row type the table declares for this shape.
     *
     * @param message the message being read
     * @return the row type
     */
    public RowType rowType(BenchMessage message) {
      return message.rowType();
    }

    /**
     * The deserialization schema the source would build for this shape. Override in a constant
     * whose schema needs more than a row type (for example projected fields).
     *
     * @param message the message being read
     * @param config the format config
     * @param registry the in-memory registry
     * @return an unopened schema
     */
    public ProtoRowDataDeserializationSchema create(
        BenchMessage message, ProtoConfluentFormatConfig config, SchemaRegistryClient registry) {
      return new InMemoryDeserializationSchema(rowType(message), config, registry);
    }
  }

  /** The message the record carries. */
  @Param({"SIMPLE", "BIG", "REPEATED_NESTED", "MAP"})
  public BenchMessage message;

  /** How the record is read. */
  @Param({"FULL", "PROJECTED"})
  public ReadShape shape;

  private ProtoRowDataDeserializationSchema schema;
  private byte[] record;

  /**
   * Registers the schema, opens the source schema and reads the record once, so the writer schema
   * lookup and the converter build happen here and not in the measurement.
   *
   * @throws IOException if the record cannot be read
   */
  @Setup(Level.Trial)
  public void setUp() throws IOException {
    BenchRegistry registry = new BenchRegistry(message);
    record = registry.record();
    schema = shape.create(message, registry.formatConfig(), registry.client());
    schema.open(null);
    if (schema.deserialize(record) == null) {
      throw new IllegalStateException("benchmark record did not deserialize: " + message);
    }
  }

  /** Closes the schema. */
  @TearDown(Level.Trial)
  public void tearDown() {
    schema.close();
  }

  /**
   * One record from wire bytes to a row.
   *
   * @return the row, consumed by JMH
   * @throws IOException if the record cannot be read
   */
  @Benchmark
  public RowData deserialize() throws IOException {
    return schema.deserialize(record);
  }
}
