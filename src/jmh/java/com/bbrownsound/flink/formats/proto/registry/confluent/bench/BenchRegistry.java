package com.bbrownsound.flink.formats.proto.registry.confluent.bench;

import com.bbrownsound.flink.formats.proto.registry.confluent.ProtoConfluentFormatOptions;
import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;

/**
 * An in-memory Schema Registry (Confluent's {@link MockSchemaRegistryClient}) with one message's
 * schema registered under the benchmark topic, and the Confluent wire-format bytes of its sample
 * record. Built once per trial, so neither the registry nor the producer side is measured.
 */
public final class BenchRegistry {

  /** Topic the records are written to; the subject is {@code bench-value}. */
  public static final String TOPIC = "bench";

  /** URL handed to the format; never contacted, the client seam returns the in-memory registry. */
  public static final String URL = "mock://bench";

  private final BenchMessage message;
  private final SchemaRegistryClient client;
  private final byte[] record;

  /**
   * Registers the message's schema and serializes its sample record the way a Confluent producer
   * does.
   *
   * @param message the message to register and serialize
   */
  public BenchRegistry(BenchMessage message) {
    this.message = message;
    MockSchemaRegistryClient registry =
        new MockSchemaRegistryClient(List.of(new ProtobufSchemaProvider()));
    try (KafkaProtobufSerializer<Message> producer = new KafkaProtobufSerializer<>(registry)) {
      // Registered normalized, as the format looks schemas up normalized (normalize-schemas).
      producer.configure(
          Map.of(
              "schema.registry.url",
              URL,
              "auto.register.schemas",
              "true",
              "normalize.schemas",
              "true"),
          false);
      this.record = producer.serialize(TOPIC, message.sample());
    }
    this.client = CachingRegistryClient.wrap(registry);
  }

  /**
   * The in-memory registry the format is given. Repeated schema and ID lookups are answered from
   * memory, as the production client answers them, so they stay out of the measurement.
   *
   * @return the registry client
   */
  public SchemaRegistryClient client() {
    return client;
  }

  /**
   * The sample record in Confluent wire format (magic byte, schema ID, message indexes, payload).
   *
   * @return a copy of the record bytes
   */
  public byte[] record() {
    return record.clone();
  }

  /**
   * The format config of a table with only {@code url} and {@code topic} set, plus {@code
   * message-class} for a message that is not the first one in its schema (the sink would otherwise
   * serialize against the first one). Every other option has its production default.
   *
   * @return a fresh config
   */
  public ProtoConfluentFormatConfig formatConfig() {
    Configuration options = new Configuration();
    options.set(ProtoConfluentFormatOptions.URL, URL);
    options.set(ProtoConfluentFormatOptions.TOPIC, TOPIC);
    if (message.pinMessageClass()) {
      options.set(ProtoConfluentFormatOptions.MESSAGE_CLASS, message.sample().getClass().getName());
    }
    return new ProtoConfluentFormatConfig(options);
  }
}
