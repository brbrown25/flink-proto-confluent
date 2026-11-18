package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * The {@code Main-Class} of the shadow JAR, so that {@code java -jar
 * flink-proto-confluent-<version>-all.jar ddl ...} works with nothing else on the classpath.
 *
 * <p>The shadow JAR leaves out SLF4J because Flink provides it, but the Schema Registry client
 * needs the SLF4J API to load. The JAR therefore carries a copy of {@code slf4j-api} as a nested
 * JAR at {@value #EMBEDDED_SLF4J}, under a name that no class loader picks up, so it is invisible
 * when the JAR sits in Flink's {@code lib/}. When SLF4J is missing from the classpath, this class
 * extracts it to a temporary file and runs {@link ProtoConfluentCli} in a class loader over the
 * shadow JAR and that file. When SLF4J is present (a classpath that has Flink or a logging setup
 * on it), it runs {@link ProtoConfluentCli} directly.
 */
public final class Main {

  static final String EMBEDDED_SLF4J = "META-INF/proto-confluent-cli/slf4j-api.jar.bin";

  private static final String CLI_CLASS =
      "com.bbrownsound.flink.formats.proto.registry.confluent.cli.ProtoConfluentCli";

  private Main() {}

  /**
   * Runs {@link ProtoConfluentCli}.
   *
   * @param args the command and its options
   * @throws Exception if the CLI cannot be started
   */
  public static void main(String[] args) throws Exception {
    if (isLoadable("org.slf4j.LoggerFactory", Main.class.getClassLoader())) {
      ProtoConfluentCli.main(args);
      return;
    }
    // SLF4J without a provider logs nothing, but reports that it found none; keep that quiet.
    if (System.getProperty("slf4j.internal.verbosity") == null) {
      System.setProperty("slf4j.internal.verbosity", "ERROR");
    }
    ClassLoader loader = withEmbeddedSlf4j();
    Thread.currentThread().setContextClassLoader(loader);
    try {
      Class.forName(CLI_CLASS, true, loader)
          .getMethod("main", String[].class)
          .invoke(null, (Object) args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw e;
    }
  }

  static boolean isLoadable(String className, ClassLoader loader) {
    try {
      Class.forName(className, false, loader);
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }

  private static ClassLoader withEmbeddedSlf4j() throws IOException, URISyntaxException {
    URL self = Main.class.getProtectionDomain().getCodeSource().getLocation();
    Path slf4j = Files.createTempFile("proto-confluent-cli-slf4j-api", ".jar");
    slf4j.toFile().deleteOnExit();
    try (InputStream embedded = Main.class.getClassLoader().getResourceAsStream(EMBEDDED_SLF4J)) {
      if (embedded == null) {
        throw new IllegalStateException(
            "SLF4J is not on the classpath and "
                + Paths.get(self.toURI())
                + " does not carry "
                + EMBEDDED_SLF4J
                + "; run the shadow (-all) JAR, or add slf4j-api to the classpath");
      }
      Files.copy(embedded, slf4j, StandardCopyOption.REPLACE_EXISTING);
    }
    return new URLClassLoader(
        new URL[] {self, slf4j.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
  }
}
