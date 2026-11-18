package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/**
 * Runs {@link Main} in-process. The fallback path is exercised the way {@code java -jar} hits it: a
 * class loader that cannot see SLF4J, a CLI classpath without {@code slf4j-api}, and the nested
 * {@code slf4j-api} JAR as a resource.
 */
class MainTest {

  private final ByteArrayOutputStream out = new ByteArrayOutputStream();
  private final ByteArrayOutputStream err = new ByteArrayOutputStream();
  private final List<Integer> exits = new ArrayList<>();
  private PrintStream originalOut;
  private PrintStream originalErr;
  private IntConsumer originalExit;
  private String originalVerbosity;

  @TempDir Path temp;

  @BeforeEach
  void captureOutputAndExit() {
    originalOut = System.out;
    originalErr = System.err;
    originalExit = Main.exit;
    originalVerbosity = System.getProperty(Main.SLF4J_VERBOSITY);
    System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    setExit(exits::add);
  }

  @AfterEach
  void restore() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    setExit(originalExit);
    if (originalVerbosity == null) {
      System.clearProperty(Main.SLF4J_VERBOSITY);
    } else {
      System.setProperty(Main.SLF4J_VERBOSITY, originalVerbosity);
    }
  }

  @Test
  void mainRunsTheCliDirectlyWhenSlf4jIsOnTheClasspathAndExitsWithItsStatus() throws Exception {
    Main.main(new String[] {"--help"});

    assertEquals(List.of(ProtoConfluentCli.EXIT_OK), exits);
    assertEquals(ProtoConfluentCli.USAGE + "\n", out.toString(StandardCharsets.UTF_8));

    out.reset();
    Main.main(new String[] {"nope"});
    assertEquals(List.of(ProtoConfluentCli.EXIT_OK, ProtoConfluentCli.EXIT_USAGE), exits);
  }

  @Test
  void runsTheCliWithTheEmbeddedSlf4jWhenSlf4jIsMissing() throws Exception {
    System.setProperty(Main.SLF4J_VERBOSITY, "ERROR");
    ClassLoader context = Thread.currentThread().getContextClassLoader();

    // An unreachable registry still builds the Schema Registry client, which needs SLF4J.
    int status =
        Main.launch(
            new String[] {"ddl", "--url", "http://127.0.0.1:1", "--subject", "x", "--columns-only"},
            loaderWithEmbeddedSlf4j(),
            classpathWithoutSlf4j());

    assertEquals(ProtoConfluentCli.EXIT_FAILURE, status);
    assertTrue(
        err.toString(StandardCharsets.UTF_8)
            .startsWith("error: Cannot reach the Schema Registry to read subject 'x'"),
        err.toString(StandardCharsets.UTF_8));
    assertEquals("ERROR", System.getProperty(Main.SLF4J_VERBOSITY), "a preset value is kept");
    assertSame(context, Thread.currentThread().getContextClassLoader());
  }

  @Test
  void quietensSlf4jAndFailsClearlyWithoutTheEmbeddedJar() throws Exception {
    System.clearProperty(Main.SLF4J_VERBOSITY);
    URL shadowJar = temp.resolve("not-the-shadow.jar").toUri().toURL();
    // The platform class loader sees neither SLF4J nor the nested JAR.
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                Main.launch(
                    new String[] {"--help"}, ClassLoader.getPlatformClassLoader(), shadowJar));
    assertTrue(e.getMessage().startsWith("SLF4J is not on the classpath and ["), e.getMessage());
    assertTrue(e.getMessage().contains("not-the-shadow.jar] does not carry "), e.getMessage());
    assertEquals("ERROR", System.getProperty(Main.SLF4J_VERBOSITY));
  }

  @Test
  void detectsWhetherAClassIsLoadable() {
    assertTrue(Main.isLoadable(Main.SLF4J_CLASS, getClass().getClassLoader()));
    assertFalse(Main.isLoadable("com.example.Missing", getClass().getClassLoader()));
  }

  private static void setExit(IntConsumer exit) {
    Main.exit = exit;
  }

  /** A class loader that holds slf4j-api only as the nested resource, like the shadow JAR. */
  private ClassLoader loaderWithEmbeddedSlf4j() throws Exception {
    Path resources = temp.resolve("resources");
    Files.createDirectories(resources.resolve("META-INF").resolve("proto-confluent-cli"));
    Files.copy(slf4jApiJar(), resources.resolve(Main.EMBEDDED_SLF4J));
    return new URLClassLoader(
        new URL[] {resources.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
  }

  /** The test classpath (main classes and dependencies) minus slf4j-api, standing in for the JAR. */
  private static URL[] classpathWithoutSlf4j() throws Exception {
    Path slf4jApi = slf4jApiJar();
    List<URL> urls = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      Path path = Paths.get(entry);
      if (!entry.isEmpty() && !path.toAbsolutePath().equals(slf4jApi.toAbsolutePath())) {
        urls.add(path.toUri().toURL());
      }
    }
    return urls.toArray(new URL[0]);
  }

  private static Path slf4jApiJar() throws Exception {
    Path jar =
        Paths.get(LoggerFactory.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    assertTrue(jar.getFileName().toString().startsWith("slf4j-api"), jar.toString());
    return jar;
  }
}
