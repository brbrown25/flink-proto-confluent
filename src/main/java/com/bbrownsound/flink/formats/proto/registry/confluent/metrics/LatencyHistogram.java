package com.bbrownsound.flink.formats.proto.registry.confluent.metrics;

import java.util.Arrays;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/**
 * A histogram over the most recent {@value #WINDOW_SIZE} values.
 *
 * <p>Flink's metric API ({@code flink-metrics-core}) has the {@link Histogram} interface but no
 * implementation; the ones Flink ships live in {@code flink-runtime} and in the optional Dropwizard
 * wrapper, neither of which a format should depend on. Registry requests are rare (the client
 * caches), so a small fixed window is enough and costs a few kilobytes per format instance.
 */
final class LatencyHistogram implements Histogram {

  /** Number of most recent values the statistics are computed over. */
  static final int WINDOW_SIZE = 1024;

  private final long[] window = new long[WINDOW_SIZE];
  private long count;

  @Override
  public synchronized void update(long value) {
    window[(int) (count % WINDOW_SIZE)] = value;
    count++;
  }

  /** Number of values ever recorded, not only those still in the window. */
  @Override
  public synchronized long getCount() {
    return count;
  }

  @Override
  public synchronized HistogramStatistics getStatistics() {
    // Until the window has wrapped, only its first 'count' slots hold values.
    long[] values = Arrays.copyOf(window, (int) Math.min(count, WINDOW_SIZE));
    Arrays.sort(values);
    return new WindowStatistics(values);
  }

  /** Statistics over a sorted copy of the window; every statistic of an empty window is zero. */
  private static final class WindowStatistics extends HistogramStatistics {
    private final long[] sorted;

    WindowStatistics(long[] sorted) {
      this.sorted = sorted;
    }

    /** Nearest-rank quantile: the smallest value with at least {@code quantile} of all below. */
    @Override
    public double getQuantile(double quantile) {
      if (sorted.length == 0) {
        return 0.0;
      }
      int rank = (int) Math.ceil(quantile * sorted.length);
      return sorted[Math.min(Math.max(rank, 1), sorted.length) - 1];
    }

    @Override
    public long[] getValues() {
      return sorted.clone();
    }

    @Override
    public int size() {
      return sorted.length;
    }

    @Override
    public double getMean() {
      return Arrays.stream(sorted).average().orElse(0.0);
    }

    /** Sample standard deviation; zero for fewer than two values. */
    @Override
    public double getStdDev() {
      if (sorted.length < 2) {
        return 0.0;
      }
      double mean = getMean();
      double squares = Arrays.stream(sorted).mapToDouble(v -> (v - mean) * (v - mean)).sum();
      return Math.sqrt(squares / (sorted.length - 1));
    }

    @Override
    public long getMax() {
      return Arrays.stream(sorted).max().orElse(0L);
    }

    @Override
    public long getMin() {
      return Arrays.stream(sorted).min().orElse(0L);
    }
  }
}
