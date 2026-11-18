package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.Map;
import org.apache.flink.table.types.logical.RowType;

/**
 * The production serialization schema with its registry client seam pointed at an in-memory
 * registry, so a benchmark measures the per-record path and not HTTP. Lives in the schema's package
 * because the seam is package-private.
 */
public final class InMemorySerializationSchema extends ProtoRowDataSerializationSchema {
  private static final long serialVersionUID = 1L;

  private final transient SchemaRegistryClient registry;

  /**
   * Creates the schema.
   *
   * @param rowType the row type of the rows to serialize
   * @param config the format config
   * @param registry the in-memory registry the subject's latest schema is read from
   */
  public InMemorySerializationSchema(
      RowType rowType, ProtoConfluentFormatConfig config, SchemaRegistryClient registry) {
    super(rowType, config);
    this.registry = registry;
  }

  @Override
  SchemaRegistryClient createSchemaRegistryClient(Map<String, String> clientProperties) {
    return registry;
  }
}
