package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.function.IntConsumer;

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

  static final String SLF4J_CLASS = "org.slf4j.LoggerFactory";

  static final String SLF4J_VERBOSITY = "slf4j.internal.verbosity";

  private static final String CLI_CLASS =
      "com.bbrownsound.flink.formats.proto.registry.confluent.cli.ProtoConfluentCli";

  /** Ends the JVM with the command's status; tests replace it to run {@link #main} in-process. */
  static IntConsumer exit = System::exit;

  private Main() {}

  /**
   * Runs {@link ProtoConfluentCli} and exits with its status.
   *
   * @param args the command and its options
   * @throws Exception if the CLI cannot be started
   */
  public static void main(String[] args) throws Exception {
    URL self = Main.class.getProtectionDomain().getCodeSource().getLocation();
    exit.accept(launch(args, Main.class.getClassLoader(), self));
  }

  /**
   * Runs {@link ProtoConfluentCli#execute(String...)}: directly when {@code classLoader} can load
   * SLF4J, otherwise in a new class loader over {@code cliClasspath} and the copy of {@code
   * slf4j-api} that {@code classLoader} holds at {@value #EMBEDDED_SLF4J}.
   *
   * @param args the command and its options
   * @param classLoader the class loader this class runs in
   * @param cliClasspath the classpath of the CLI without SLF4J, i.e. the shadow JAR
   * @return the exit status
   * @throws Exception if the CLI cannot be started
   */
  static int launch(String[] args, ClassLoader classLoader, URL... cliClasspath) throws Exception {
    if (isLoadable(SLF4J_CLASS, classLoader)) {
      return ProtoConfluentCli.execute(args);
    }
    // SLF4J without a provider logs nothing, but reports that it found none; keep that quiet.
    if (System.getProperty(SLF4J_VERBOSITY) == null) {
      System.setProperty(SLF4J_VERBOSITY, "ERROR");
    }
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    try (URLClassLoader cliLoader = withEmbeddedSlf4j(classLoader, cliClasspath)) {
      thread.setContextClassLoader(cliLoader);
      return (Integer)
          Class.forName(CLI_CLASS, true, cliLoader)
              .getMethod("execute", String[].class)
              .invoke(null, (Object) args);
    } finally {
      thread.setContextClassLoader(previous);
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

  private static URLClassLoader withEmbeddedSlf4j(ClassLoader classLoader, URL... cliClasspath)
      throws IOException {
    Path slf4j = Files.createTempFile("proto-confluent-cli-slf4j-api", ".jar");
    slf4j.toFile().deleteOnExit();
    try (InputStream embedded = classLoader.getResourceAsStream(EMBEDDED_SLF4J)) {
      if (embedded == null) {
        throw new IllegalStateException(
            "SLF4J is not on the classpath and "
                + Arrays.toString(cliClasspath)
                + " does not carry "
                + EMBEDDED_SLF4J
                + "; run the shadow (-all) JAR, or add slf4j-api to the classpath");
      }
      Files.copy(embedded, slf4j, StandardCopyOption.REPLACE_EXISTING);
    }
    URL[] urls = Arrays.copyOf(cliClasspath, cliClasspath.length + 1);
    urls[cliClasspath.length] = slf4j.toUri().toURL();
    return new URLClassLoader(urls, ClassLoader.getPlatformClassLoader());
  }
}
