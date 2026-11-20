package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.REGISTRY_URL;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.decode;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.field;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.row;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import javax.tools.ToolProvider;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SimpleUserCodeClassLoader;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The sink loads {@code message-class} with the job's user-code class loader (GitHub issue #242),
 * so a generated class shipped in the job JAR works while the format itself sits in Flink's {@code
 * lib/}. The class used here is compiled into a temporary directory that only the "job" class
 * loader sees, standing in for the job JAR.
 */
class ProtoRowDataSerializationSchemaClassLoaderTest {

  /** A message class that exists only in the job's class loader, not in the format's. */
  private static final String JOB_CLASS = "com.example.jobjar.JobOrder";

  private static final RowType ROW_TYPE = rowType("content", "date_time");

  @TempDir
  Path temp;

  private URLClassLoader jobLoader;
  private StubSchemaRegistryClient registry;

  @BeforeEach
  void compileJobClass() throws IOException {
    Path source = temp.resolve("src").resolve("JobOrder.java");
    Files.createDirectories(source.getParent());
    Files.writeString(
        source,
        "package com.example.jobjar;\n"
            + "public final class JobOrder {\n"
            + "  private JobOrder() {}\n"
            + "  public static com.google.protobuf.Message getDefaultInstance() {\n"
            + "    return "
            + TestSimple.SimpleMessage.class.getCanonicalName()
            + ".getDefaultInstance();\n"
            + "  }\n"
            + "}\n");
    Path classes = temp.resolve("classes");
    Files.createDirectories(classes);
    int status =
        ToolProvider.getSystemJavaCompiler()
            .run(
                null,
                null,
                null,
                "-proc:none",
                "-classpath",
                System.getProperty("java.class.path"),
                "-d",
                classes.toString(),
                source.toString());
    assertEquals(0, status, "the stand-in for the job JAR compiles");
    jobLoader = new URLClassLoader(new URL[] {classes.toUri().toURL()}, getClass().getClassLoader());
    registry = new StubSchemaRegistryClient();
  }

  @AfterEach
  void closeJobLoader() throws IOException {
    jobLoader.close();
  }

  private ProtoRowDataSerializationSchema schema() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, REGISTRY_URL);
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    options.set(ProtoConfluentFormatOptions.AUTO_REGISTER_SCHEMAS, true);
    options.set(ProtoConfluentFormatOptions.MESSAGE_CLASS, JOB_CLASS);
    return new ProtoRowDataSerializationSchema(ROW_TYPE, new ProtoConfluentFormatConfig(options)) {
      private static final long serialVersionUID = 1L;

      @Override
      SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
        return registry;
      }
    };
  }

  private static SerializationSchema.InitializationContext context(ClassLoader classLoader) {
    return new SerializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return new UnregisteredMetricsGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return SimpleUserCodeClassLoader.create(classLoader);
      }
    };
  }

  /** The row was written with the job class's descriptor, not one derived from the table. */
  private void assertWrittenWithJobClass(byte[] bytes) {
    DynamicMessage message = decode(registry, bytes);
    assertEquals(
        TestSimple.SimpleMessage.getDescriptor().getFullName(),
        message.getDescriptorForType().getFullName());
    assertEquals("a", field(message, "content"));
    assertEquals("b", field(message, "date_time"));
  }

  @Test
  void jobClassIsNotVisibleToTheFormatsOwnClassLoader() {
    assertThrows(
        ClassNotFoundException.class,
        () -> Class.forName(JOB_CLASS, false, ProtoRowDataSerializationSchema.class.getClassLoader()));
  }

  @Test
  void open_loadsTheMessageClassWithTheUserCodeClassLoader() {
    ProtoRowDataSerializationSchema schema = schema();
    schema.open(context(jobLoader));

    assertWrittenWithJobClass(schema.serialize(row("a", "b")));
  }

  @Test
  void open_withoutContext_loadsTheMessageClassWithTheContextClassLoader() {
    ProtoRowDataSerializationSchema schema = schema();
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(jobLoader);
    try {
      schema.open(null);
    } finally {
      thread.setContextClassLoader(previous);
    }

    assertWrittenWithJobClass(schema.serialize(row("a", "b")));
  }

  @Test
  void open_failsClearly_whenTheUserCodeClassLoaderCannotSeeTheClass() {
    ProtoRowDataSerializationSchema schema = schema();
    ValidationException e =
        assertThrows(
            ValidationException.class,
            () -> schema.open(context(ProtoRowDataSerializationSchema.class.getClassLoader())));
    assertTrue(e.getMessage().contains(JOB_CLASS), e.getMessage());
    assertTrue(e.getCause() instanceof ClassNotFoundException, String.valueOf(e.getCause()));
  }
}
