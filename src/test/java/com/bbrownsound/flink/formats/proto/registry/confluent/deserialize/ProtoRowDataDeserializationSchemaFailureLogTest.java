package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.OnDeserializeError;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/**
 * GitHub issue #233: with {@code on-deserialize-error = skip}, a topic full of bad records must not
 * write an {@code ERROR} with a stack trace per record. The first failure is logged, then at most
 * one per interval, with the number of failures suppressed in between.
 */
class ProtoRowDataDeserializationSchemaFailureLogTest {

  private static final byte[] INVALID = new byte[] {0, 1, 2, 3, 4};

  /** Records each failure the schema logs at ERROR, as (deserializeCount, suppressed) pairs. */
  private static final class RecordingSchema extends ProtoRowDataDeserializationSchema {
    private static final long serialVersionUID = 1L;
    final transient AtomicLong clock = new AtomicLong();
    final transient List<long[]> logged = new ArrayList<>();

    RecordingSchema(ProtoConfluentFormatConfig config) {
      super(rowType(), null, config);
    }

    @Override
    FailureLogLimiter createFailureLogLimiter() {
      return new FailureLogLimiter(FailureLogLimiter.DEFAULT_INTERVAL, clock::get);
    }

    @Override
    void logDeserializeFailure(byte[] message, long n, long suppressed, Exception e) {
      logged.add(new long[] {n, suppressed});
      super.logDeserializeFailure(message, n, suppressed, e);
    }
  }

  private static ProtoConfluentFormatConfig config(OnDeserializeError onError) {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig(
            "http://localhost:8081",
            "test-topic",
            false,
            Map.of("schema.registry.url", "http://localhost:8081"));
    config.onDeserializeError = onError;
    return config;
  }

  private static RowType rowType() {
    return (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor());
  }

  @Test
  void skip_logsTheFirstFailureThenAtMostOnePerIntervalWithTheSuppressedCount()
      throws IOException {
    var schema = new RecordingSchema(config(OnDeserializeError.SKIP));
    schema.open(null);

    for (int i = 0; i < 50; i++) {
      assertNull(schema.deserialize(INVALID));
    }
    assertEquals(1, schema.logged.size(), "one ERROR for 50 failures within the interval");
    assertEquals(1L, schema.logged.get(0)[0]);
    assertEquals(0L, schema.logged.get(0)[1]);

    // Just short of the interval: still suppressed.
    schema.clock.set(FailureLogLimiter.DEFAULT_INTERVAL.toNanos() - 1);
    assertNull(schema.deserialize(INVALID));
    assertEquals(1, schema.logged.size());

    // Once the interval has passed, the next failure is logged with the 50 suppressed before it.
    schema.clock.set(FailureLogLimiter.DEFAULT_INTERVAL.toNanos());
    assertNull(schema.deserialize(INVALID));
    assertEquals(2, schema.logged.size());
    assertEquals(52L, schema.logged.get(1)[0]);
    assertEquals(50L, schema.logged.get(1)[1]);
  }

  @Test
  void fail_logsTheFailureThatFailsTheTask() throws IOException {
    var schema = new RecordingSchema(config(OnDeserializeError.FAIL));
    schema.open(null);

    assertThrows(IOException.class, () -> schema.deserialize(INVALID));
    assertEquals(1, schema.logged.size());
    assertEquals(0L, schema.logged.get(0)[1]);
  }

  @Test
  void defaultLimiter_usesTheDefaultIntervalOnTheSystemClock() throws IOException {
    var schema = new ProtoRowDataDeserializationSchema(rowType(), null, config(OnDeserializeError.SKIP));
    FailureLogLimiter limiter = schema.createFailureLogLimiter();
    assertEquals(0L, limiter.tryAcquire());
    assertEquals(FailureLogLimiter.SUPPRESSED, limiter.tryAcquire());
  }

  @Test
  void limiter_countsSuppressedFailuresPerInterval() {
    AtomicLong clock = new AtomicLong(100);
    FailureLogLimiter limiter = new FailureLogLimiter(Duration.ofNanos(10), clock::get);

    assertEquals(0L, limiter.tryAcquire());
    assertEquals(FailureLogLimiter.SUPPRESSED, limiter.tryAcquire());
    assertEquals(FailureLogLimiter.SUPPRESSED, limiter.tryAcquire());
    clock.set(110);
    assertEquals(2L, limiter.tryAcquire());
    // The interval restarts at the logged failure, and the suppressed count resets.
    clock.set(119);
    assertEquals(FailureLogLimiter.SUPPRESSED, limiter.tryAcquire());
    clock.set(500);
    assertEquals(1L, limiter.tryAcquire());
    clock.set(510);
    assertEquals(0L, limiter.tryAcquire());
  }
}
