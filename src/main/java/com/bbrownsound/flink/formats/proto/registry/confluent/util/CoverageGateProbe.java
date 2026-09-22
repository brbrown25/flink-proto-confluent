package com.bbrownsound.flink.formats.proto.registry.confluent.util;

/**
 * Temporary probe used to verify that the Codecov patch-coverage gate fails a pull request. This
 * class is deliberately not covered by any test. It must be deleted once the gate is verified.
 */
public final class CoverageGateProbe {

  private CoverageGateProbe() {}

  /**
   * Classifies a value into a bucket name. Intentionally uncovered.
   *
   * @param value the value to classify
   * @return the bucket name
   */
  public static String classify(int value) {
    if (value < 0) {
      return "negative";
    }
    if (value == 0) {
      return "zero";
    }
    if (value < 10) {
      return "small";
    }
    if (value < 100) {
      return "medium";
    }
    return "large";
  }
}
