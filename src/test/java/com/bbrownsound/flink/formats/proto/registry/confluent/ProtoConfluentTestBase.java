package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.awaitility.Awaitility.await;

import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.Collections;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public abstract class ProtoConfluentTestBase {

  private static final Network network = Network.newNetwork();

  @Container
  static KafkaContainer kafka =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"))
          .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
          .withNetwork(network)
          .withExposedPorts(9093)
          .withNetworkAliases("kafka");

  @Container
  static GenericContainer<?> schemaRegistry =
      new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.0"))
          .withNetwork(network)
          .withExposedPorts(8081)
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
          .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
          .dependsOn(kafka)
          .waitingFor(
              Wait.forHttp("/subjects")
                  .forStatusCode(200)
                  .withStartupTimeout(java.time.Duration.ofSeconds(120)));

  protected final String getSchemaRegistryUrl() {
    return "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getMappedPort(8081);
  }

  protected final Map<String, String> getConfig() {
    return Map.ofEntries(
        Map.entry("schema.registry.url", getSchemaRegistryUrl()),
        Map.entry("auto.register.schemas", "true"));
  }

  protected final CachedSchemaRegistryClient getSchemaRegistryClient() {
    return new CachedSchemaRegistryClient(
        getSchemaRegistryUrl(),
        100,
        Collections.singletonList(new ProtobufSchemaProvider()),
        getConfig());
  }

  protected final <T extends Message> byte[] serializeMessage(T message, String topic)
      throws Exception {
    CachedSchemaRegistryClient client = getSchemaRegistryClient();
    KafkaProtobufSerializer<T> serializer = new KafkaProtobufSerializer<>(client);
    serializer.configure(getConfig(), false);
    byte[][] result = new byte[1][];
    await()
        .atMost(java.time.Duration.ofSeconds(30))
        .pollInterval(java.time.Duration.ofMillis(500))
        .until(
            () -> {
              try {
                result[0] = serializer.serialize(topic, message);
                return result[0] != null;
              } catch (Exception e) {
                return false;
              }
            });
    return result[0];
  }
}
