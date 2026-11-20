package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import java.time.Duration;
import java.util.function.LongSupplier;

/**
 * Decides which deserialize failures the source logs at {@code ERROR} with a stack trace: the first
 * one, then at most one per interval. The others are suppressed and counted, and the next logged
 * failure reports how many were suppressed since the previous one. With {@code
 * on-deserialize-error = skip} on a topic with many bad records this keeps the TaskManager log from
 * growing by a stack trace per record (GitHub issue #233); {@code numDeserializeErrors} still
 * counts every failure.
 *
 * <p>Thread-safe, although a source calls it from one thread.
 */
final class FailureLogLimiter {
  /** How often a source logs a deserialize failure with its stack trace, at most. */
  static final Duration DEFAULT_INTERVAL = Duration.ofMinutes(1);

  /** Returned by {@link #tryAcquire()} when the failure must not be logged. */
  static final long SUPPRESSED = -1L;

  private final long intervalNanos;
  private final LongSupplier nanoClock;
  private boolean loggedAny;
  private long lastLoggedNanos;
  private long suppressedSinceLastLog;

  /**
   * Creates a limiter.
   *
   * @param interval the minimum time between two logged failures
   * @param nanoClock a monotonic clock in nanoseconds, such as {@code System::nanoTime}
   */
  FailureLogLimiter(Duration interval, LongSupplier nanoClock) {
    this.intervalNanos = interval.toNanos();
    this.nanoClock = nanoClock;
  }

  /**
   * Records a failure and says whether to log it.
   *
   * @return {@link #SUPPRESSED} when the failure must not be logged; otherwise the number of
   *     failures suppressed since the previous logged one (zero for the first failure)
   */
  synchronized long tryAcquire() {
    final long now = nanoClock.getAsLong();
    if (loggedAny && now - lastLoggedNanos < intervalNanos) {
      suppressedSinceLastLog++;
      return SUPPRESSED;
    }
    final long suppressed = suppressedSinceLastLog;
    loggedAny = true;
    lastLoggedNanos = now;
    suppressedSinceLastLog = 0;
    return suppressed;
  }
}
