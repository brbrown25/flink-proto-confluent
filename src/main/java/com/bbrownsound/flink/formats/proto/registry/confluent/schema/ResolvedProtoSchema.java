package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoSqlType;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToSqlType;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.RecursionStrategy;
import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.Objects;

/**
 * A protobuf schema fetched from the Schema Registry, with its references resolved, and the
 * message within it that a table reads.
 */
public final class ResolvedProtoSchema {

  private final String subject;
  private final int version;
  private final int id;
  private final ProtobufSchema schema;
  private final Descriptor descriptor;

  /**
   * Creates a resolved schema.
   *
   * @param subject the subject the schema was read from
   * @param version the subject version
   * @param id the global schema ID
   * @param schema the parsed schema, with references resolved
   * @param descriptor the descriptor of the selected message
   */
  public ResolvedProtoSchema(
      String subject, int version, int id, ProtobufSchema schema, Descriptor descriptor) {
    this.subject = Objects.requireNonNull(subject, "subject");
    this.version = version;
    this.id = id;
    this.schema = Objects.requireNonNull(schema, "schema");
    this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
  }

  /** Returns the subject the schema was read from. */
  public String getSubject() {
    return subject;
  }

  /** Returns the subject version. */
  public int getVersion() {
    return version;
  }

  /** Returns the global schema ID. */
  public int getId() {
    return id;
  }

  /** Returns the parsed schema, with its references resolved. */
  public ProtobufSchema getSchema() {
    return schema;
  }

  /** Returns the descriptor of the selected message. */
  public Descriptor getDescriptor() {
    return descriptor;
  }

  /** Returns the fully-qualified name of the selected message. */
  public String getMessageName() {
    return descriptor.getFullName();
  }

  /**
   * Returns the table row type of the selected message. Inside Flink, convert it with {@code
   * ProtoToLogicalType.toLogicalType(...)}.
   */
  public ProtoSqlType toSqlType() {
    return ProtoToSqlType.toSqlType(descriptor);
  }

  /**
   * Returns the table row type of the selected message, mapping each field that makes a message
   * contain itself as {@code recursion} says (see {@link ProtoToSqlType#toSqlType(Descriptor,
   * RecursionStrategy)}).
   *
   * @param recursion how to map a recursive field
   * @return the row type
   */
  public ProtoSqlType toSqlType(RecursionStrategy recursion) {
    return ProtoToSqlType.toSqlType(descriptor, recursion);
  }
}
