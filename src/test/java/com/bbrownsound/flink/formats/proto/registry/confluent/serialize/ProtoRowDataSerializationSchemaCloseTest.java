package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.REGISTRY_URL;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.TOPIC;
import static com.bbrownsound.flink.formats.proto.registry.confluent.serialize.SinkTestSupport.rowType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Covers GitHub issue #248 on the sink: the Schema Registry client the schema creates in {@code
 * open} is closed by the user-code class loader release hook and by {@code close()}.
 */
@DisplayName("ProtoRowDataSerializationSchema closes its Schema Registry client (issue #248)")
class ProtoRowDataSerializationSchemaCloseTest {

  private final RecordingClassLoader loader = new RecordingClassLoader();
  private final MetricListener listener = new MetricListener();

  @Test
  void open_registersAReleaseHookThatClosesTheClient() {
    ClientRecordingSchema schema = schema();
    schema.open(context());

    assertEquals(1, loader.hooks.size());
    String name = loader.hooks.keySet().iterator().next();
    assertTrue(name.startsWith("proto-confluent-sink-"), name);
    assertEquals(0, schema.clients.get(0).closed);

    // What Flink does when the job's user-code class loader is released on the TaskManager.
    loader.hooks.values().forEach(Runnable::run);
    assertEquals(1, schema.clients.get(0).closed);
  }

  @Test
  void eachSchemaInstanceRegistersItsOwnReleaseHook() {
    // Hooks are registered "if absent" by name: a shared name would leave the clients of all but
    // the first parallel subtask on a TaskManager unclosed.
    ClientRecordingSchema first = schema();
    ClientRecordingSchema second = schema();
    first.open(context());
    second.open(context());

    assertEquals(2, loader.hooks.size());
    loader.hooks.values().forEach(Runnable::run);
    assertEquals(1, first.clients.get(0).closed);
    assertEquals(1, second.clients.get(0).closed);
  }

  @Test
  void open_isIdempotent_andDoesNotCreateASecondClientOrHook() {
    ClientRecordingSchema schema = schema();
    schema.open(context());
    schema.open(context());
    assertEquals(1, schema.clients.size());
    assertEquals(1, loader.hooks.size());
  }

  @Test
  void close_closesTheClientOnce_andCanBeCalledAgain() {
    ClientRecordingSchema schema = schema();
    schema.open(context());

    schema.close();
    schema.close();
    loader.hooks.values().forEach(Runnable::run);

    assertEquals(1, schema.clients.get(0).closed);
  }

  @Test
  void close_worksAsTryWithResources_withoutAnInitializationContext() {
    ClientRecordingSchema schema = schema();
    try (ClientRecordingSchema opened = schema) {
      opened.open(null);
    }
    assertEquals(1, schema.clients.get(0).closed);
    assertTrue(loader.hooks.isEmpty());
  }

  @Test
  void open_afterClose_createsAFreshClient() {
    ClientRecordingSchema schema = schema();
    schema.open(context());
    schema.close();
    schema.open(context());

    assertEquals(2, schema.clients.size());
    assertEquals(0, schema.clients.get(1).closed);
  }

  @Test
  void close_beforeOpen_isANoOp() {
    ClientRecordingSchema schema = schema();
    assertDoesNotThrow(schema::close);
    assertTrue(schema.clients.isEmpty());
  }

  @Test
  void close_swallowsAClientCloseFailure() {
    ClientRecordingSchema schema = schema();
    schema.closeFailure = new IOException("close failed");
    schema.open(context());
    // A release hook must not be derailed by the client refusing to close.
    assertDoesNotThrow(schema::close);
    assertEquals(1, schema.clients.get(0).closed);
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static ClientRecordingSchema schema() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, REGISTRY_URL);
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    return new ClientRecordingSchema(new ProtoConfluentFormatConfig(options));
  }

  private SerializationSchema.InitializationContext context() {
    return new SerializationSchema.InitializationContext() {
      @Override
      public MetricGroup getMetricGroup() {
        return listener.getMetricGroup();
      }

      @Override
      public UserCodeClassLoader getUserCodeClassLoader() {
        return loader;
      }
    };
  }

  /** Hands out in-memory clients that count how often they are closed. */
  private static final class ClientRecordingSchema extends ProtoRowDataSerializationSchema {
    private static final long serialVersionUID = 1L;

    private final transient List<ClosingClient> clients = new ArrayList<>();
    private transient IOException closeFailure;

    ClientRecordingSchema(ProtoConfluentFormatConfig config) {
      super(rowType("content", "date_time"), config);
    }

    @Override
    SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
      ClosingClient client = new ClosingClient(closeFailure);
      clients.add(client);
      return client;
    }
  }

  /** An in-memory registry client that counts {@code close()} calls and can fail them. */
  private static final class ClosingClient extends MockSchemaRegistryClient {
    private final IOException closeFailure;
    private int closed;

    ClosingClient(IOException closeFailure) {
      this.closeFailure = closeFailure;
    }

    @Override
    public void close() throws IOException {
      closed++;
      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }

  /** Records release hooks the way Flink's user-code class loader does (first name wins). */
  private static final class RecordingClassLoader implements UserCodeClassLoader {
    final Map<String, Runnable> hooks = new LinkedHashMap<>();

    @Override
    public ClassLoader asClassLoader() {
      return ProtoRowDataSerializationSchemaCloseTest.class.getClassLoader();
    }

    @Override
    public void registerReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
      hooks.putIfAbsent(releaseHookName, releaseHook);
    }
  }
}
