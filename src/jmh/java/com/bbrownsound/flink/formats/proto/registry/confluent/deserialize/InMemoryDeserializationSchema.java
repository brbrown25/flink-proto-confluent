package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.flink.table.types.logical.RowType;

/**
 * The production deserialization schema with its registry client seam pointed at an in-memory
 * registry, so a benchmark measures the per-record path and not HTTP. Lives in the schema's package
 * because the seam is package-private.
 */
public final class InMemoryDeserializationSchema extends ProtoRowDataDeserializationSchema {
  private static final long serialVersionUID = 1L;

  private final transient SchemaRegistryClient registry;

  /**
   * Creates the schema.
   *
   * @param rowType the row type to produce
   * @param config the format config
   * @param registry the in-memory registry the writer schemas are looked up in
   */
  public InMemoryDeserializationSchema(
      RowType rowType, ProtoConfluentFormatConfig config, SchemaRegistryClient registry) {
    super(rowType, null, config);
    this.registry = registry;
  }

  @Override
  SchemaRegistryClient createSchemaRegistryClient(InitializationContext context) {
    return registry;
  }
}
