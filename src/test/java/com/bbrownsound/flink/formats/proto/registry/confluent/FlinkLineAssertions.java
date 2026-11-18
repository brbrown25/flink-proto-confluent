package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.CodeSource;
import org.apache.flink.runtime.util.EnvironmentInformation;

/**
 * Checks that a test run really uses the Flink line it was asked to test. The build passes the
 * requested versions as system properties (see build.gradle.kts); CI additionally passes its own
 * matrix value, so a leg that silently fell back to the default Flink version fails instead of
 * passing against the wrong line.
 */
public final class FlinkLineAssertions {

  private FlinkLineAssertions() {}

  /** The flinkVersion Gradle resolved for this run. Skips the check outside a Gradle run. */
  public static String requestedFlinkVersion() {
    final String version = System.getProperty("flinkLine.flinkVersion", "");
    assumeTrue(!version.isEmpty(), "not run through Gradle: flinkLine.flinkVersion is not set");
    return version;
  }

  /** The Kafka connector version Gradle selected for the requested Flink line. */
  public static String requestedKafkaConnectorVersion() {
    final String version = System.getProperty("flinkLine.kafkaConnectorVersion", "");
    assumeTrue(!version.isEmpty(), "not run through Gradle: kafkaConnectorVersion is not set");
    return version;
  }

  /** CI's matrix value, when set, must be the version Gradle resolved. */
  public static void assertMatchesCiMatrix(String requested) {
    final String ci = System.getProperty("flinkLine.ciExpectedFlinkVersion", "");
    if (!ci.isEmpty()) {
      assertEquals(
          ci,
          requested,
          "CI asked for Flink "
              + ci
              + " but Gradle resolved flinkVersion="
              + requested
              + "; was -PflinkVersion dropped?");
    }
  }

  /** The Flink runtime on the classpath reports the requested version. */
  public static void assertRuntimeVersion(String requested) {
    assertEquals(
        requested,
        EnvironmentInformation.getVersion(),
        "flink-runtime on the test classpath is not the requested Flink version");
  }

  /** The class is loaded from {@code <artifact>-<version>.jar}. */
  public static void assertLoadedFrom(Class<?> type, String artifact, String version) {
    final String jar = jarName(type);
    assertEquals(
        artifact + "-" + version + ".jar",
        jar,
        type.getName() + " was loaded from " + jar + ", not from " + artifact + " " + version);
  }

  /** A per-line connector version ({@code X.Y.Z-<flink major.minor>}) targets this Flink major. */
  public static void assertConnectorTargetsLine(String connectorVersion, String flinkVersion) {
    final int dash = connectorVersion.lastIndexOf('-');
    assertTrue(dash > 0, "connector version has no Flink suffix: " + connectorVersion);
    final String connectorFlinkMajor =
        connectorVersion.substring(dash + 1).split("\\.", -1)[0];
    final String flinkMajor = flinkVersion.split("\\.", -1)[0];
    assertEquals(
        flinkMajor,
        connectorFlinkMajor,
        "connector " + connectorVersion + " is built for another Flink line than " + flinkVersion);
  }

  private static String jarName(Class<?> type) {
    final CodeSource source = type.getProtectionDomain().getCodeSource();
    assertTrue(source != null && source.getLocation() != null, "no code source for " + type);
    try {
      final Path fileName = Path.of(source.getLocation().toURI()).getFileName();
      assertTrue(fileName != null, "code source of " + type + " has no file name");
      return String.valueOf(fileName);
    } catch (URISyntaxException e) {
      throw new IllegalStateException("bad code source for " + type, e);
    }
  }
}
