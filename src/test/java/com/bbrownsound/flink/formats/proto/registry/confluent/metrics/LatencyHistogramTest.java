package com.bbrownsound.flink.formats.proto.registry.confluent.metrics;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.metrics.HistogramStatistics;
import org.junit.jupiter.api.Test;

class LatencyHistogramTest {

  @Test
  void emptyHistogram_reportsZeroForEveryStatistic() {
    HistogramStatistics statistics = new LatencyHistogram().getStatistics();

    assertEquals(0, statistics.size());
    assertArrayEquals(new long[0], statistics.getValues());
    assertEquals(0.0, statistics.getQuantile(0.99));
    assertEquals(0.0, statistics.getMean());
    assertEquals(0.0, statistics.getStdDev());
    assertEquals(0L, statistics.getMin());
    assertEquals(0L, statistics.getMax());
  }

  @Test
  void singleValue_isEveryQuantileAndHasNoDeviation() {
    LatencyHistogram histogram = new LatencyHistogram();
    histogram.update(42);
    HistogramStatistics statistics = histogram.getStatistics();

    assertEquals(1L, histogram.getCount());
    assertEquals(42.0, statistics.getQuantile(0.0));
    assertEquals(42.0, statistics.getQuantile(0.5));
    assertEquals(42.0, statistics.getQuantile(1.0));
    assertEquals(42.0, statistics.getMean());
    assertEquals(0.0, statistics.getStdDev());
  }

  @Test
  void statistics_areComputedOverTheRecordedValues() {
    LatencyHistogram histogram = new LatencyHistogram();
    // Recorded out of order on purpose: the statistics sort a copy.
    for (long value : new long[] {40, 10, 30, 20}) {
      histogram.update(value);
    }
    HistogramStatistics statistics = histogram.getStatistics();

    assertEquals(4L, histogram.getCount());
    assertEquals(4, statistics.size());
    assertArrayEquals(new long[] {10, 20, 30, 40}, statistics.getValues());
    assertEquals(10L, statistics.getMin());
    assertEquals(40L, statistics.getMax());
    assertEquals(25.0, statistics.getMean());
    assertEquals(Math.sqrt(500.0 / 3.0), statistics.getStdDev(), 1e-9);
    assertEquals(10.0, statistics.getQuantile(0.0));
    assertEquals(20.0, statistics.getQuantile(0.5));
    assertEquals(30.0, statistics.getQuantile(0.75));
    assertEquals(40.0, statistics.getQuantile(0.99));
    assertEquals(40.0, statistics.getQuantile(1.0));
  }

  @Test
  void getValues_returnsACopy() {
    LatencyHistogram histogram = new LatencyHistogram();
    histogram.update(5);
    HistogramStatistics statistics = histogram.getStatistics();

    statistics.getValues()[0] = 99;

    assertEquals(5L, statistics.getMax());
  }

  @Test
  void window_keepsOnlyTheMostRecentValues_whileCountKeepsGrowing() {
    LatencyHistogram histogram = new LatencyHistogram();
    for (int i = 0; i < LatencyHistogram.WINDOW_SIZE; i++) {
      histogram.update(1_000);
    }
    // A full window of newer, smaller values pushes every old one out.
    for (int i = 1; i <= LatencyHistogram.WINDOW_SIZE; i++) {
      histogram.update(i);
    }
    HistogramStatistics statistics = histogram.getStatistics();

    assertEquals(2L * LatencyHistogram.WINDOW_SIZE, histogram.getCount());
    assertEquals(LatencyHistogram.WINDOW_SIZE, statistics.size());
    assertEquals(1L, statistics.getMin());
    assertEquals(LatencyHistogram.WINDOW_SIZE, statistics.getMax());
  }
}
