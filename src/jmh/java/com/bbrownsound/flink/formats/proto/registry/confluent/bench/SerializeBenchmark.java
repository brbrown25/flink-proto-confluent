package com.bbrownsound.flink.formats.proto.registry.confluent.bench;

import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.InMemoryDeserializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.deserialize.ProtoRowDataDeserializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.InMemorySerializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.ProtoRowDataSerializationSchema;
import java.io.IOException;
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

/**
 * Sink side: {@link ProtoRowDataSerializationSchema#serialize(RowData)} on one row, from the {@link
 * RowData} the table emits to Confluent wire-format bytes. The subject's latest schema is in an
 * in-memory registry and is resolved during setup, so every measured call is the steady-state
 * per-record path.
 *
 * <p>The row is the one the source produces for the same {@link BenchMessage} sample, so the two
 * benchmarks measure the two directions over the same data.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SerializeBenchmark {

  /** The message the subject's schema is. */
  @Param({"SIMPLE", "BIG", "REPEATED_NESTED", "MAP"})
  public BenchMessage message;

  private ProtoRowDataSerializationSchema schema;
  private RowData row;

  /**
   * Registers the schema, builds the row, opens the sink schema and writes the row once, so the
   * subject lookup and the converter build happen here and not in the measurement.
   *
   * @throws IOException if the sample record cannot be turned into a row
   */
  @Setup(Level.Trial)
  public void setUp() throws IOException {
    BenchRegistry registry = new BenchRegistry(message);
    RowType rowType = message.rowType();
    ProtoRowDataDeserializationSchema source =
        new InMemoryDeserializationSchema(rowType, registry.formatConfig(), registry.client());
    source.open(null);
    row = source.deserialize(registry.record());
    source.close();

    schema = new InMemorySerializationSchema(rowType, registry.formatConfig(), registry.client());
    schema.open(null);
    if (schema.serialize(row) == null) {
      throw new IllegalStateException("benchmark row did not serialize: " + message);
    }
  }

  /**
   * One row to wire bytes.
   *
   * @return the bytes, consumed by JMH
   */
  @Benchmark
  public byte[] serialize() {
    return schema.serialize(row);
  }
}
