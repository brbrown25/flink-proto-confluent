package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Fetches a protobuf schema from the Schema Registry by subject and version, resolves its
 * references, and selects the message a table reads.
 *
 * <p>Does not depend on Flink. The {@code ddl} command line tool renders the result as DDL; code
 * running inside Flink (for example a catalog) converts {@link ResolvedProtoSchema#getDescriptor()}
 * with {@code ProtoToLogicalType.toLogicalType(...)}.
 *
 * <p>Instances are thread-safe if the client is; {@link
 * io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient} is, and caches schemas by ID,
 * so reuse one resolver per registry.
 */
public final class SchemaRegistrySchemaResolver {

  /** Registry error code for a subject that does not exist. */
  static final int SUBJECT_NOT_FOUND = 40401;

  /** Registry error code for a version that does not exist. */
  static final int VERSION_NOT_FOUND = 40402;

  private static final String PROTOBUF = ProtobufSchema.TYPE;

  private final SchemaRegistryClient client;

  /**
   * Creates a resolver over an existing client. The client must have the protobuf schema provider
   * registered, as clients from {@link SchemaRegistryClients#create(String, Map)} do.
   *
   * @param client the registry client
   */
  public SchemaRegistrySchemaResolver(SchemaRegistryClient client) {
    this.client = Objects.requireNonNull(client, "client");
  }

  /**
   * Creates a resolver with its own caching client.
   *
   * @param schemaRegistryUrl the registry URL, or several separated by commas
   * @param clientProperties registry client properties, e.g. from {@link
   *     SchemaRegistryClients#clientProperties(Map)}
   * @return the resolver
   */
  public static SchemaRegistrySchemaResolver create(
      String schemaRegistryUrl, Map<String, ?> clientProperties) {
    return new SchemaRegistrySchemaResolver(
        SchemaRegistryClients.create(schemaRegistryUrl, clientProperties));
  }

  /** Returns the registry client, e.g. to list subjects. */
  public SchemaRegistryClient getClient() {
    return client;
  }

  /**
   * Resolves the latest version of a subject and selects its first message.
   *
   * @param subject the subject, e.g. {@code orders-value}
   * @return the resolved schema
   * @throws SchemaResolutionException if the schema cannot be resolved
   */
  public ResolvedProtoSchema resolve(String subject) {
    return resolve(subject, null, null);
  }

  /**
   * Resolves a subject version and selects a message in it.
   *
   * @param subject the subject, e.g. {@code orders-value}
   * @param version the subject version, or {@code null} for the latest
   * @param messageName the fully-qualified name of the message to select, or {@code null} for the
   *     first message in the schema (the one Confluent serializers write by default)
   * @return the resolved schema
   * @throws SchemaResolutionException if the subject or version does not exist, the schema is not
   *     protobuf, the message is not in it, or the registry cannot be reached
   */
  public ResolvedProtoSchema resolve(
      String subject, @Nullable Integer version, @Nullable String messageName) {
    Objects.requireNonNull(subject, "subject");
    String what =
        "subject '" + subject + "' " + (version == null ? "(latest version)" : "version " + version);
    SchemaMetadata metadata;
    ParsedSchema parsed;
    try {
      metadata =
          version == null
              ? client.getLatestSchemaMetadata(subject)
              : client.getSchemaMetadata(subject, version);
      String type = metadata.getSchemaType();
      if (!PROTOBUF.equals(type)) {
        throw new SchemaResolutionException(
            "Schema of " + what + " is " + (type == null ? "AVRO" : type) + ", not PROTOBUF",
            false,
            null);
      }
      // Fetching by ID parses the schema with the protobuf provider, which resolves its references.
      parsed = client.getSchemaBySubjectAndId(subject, metadata.getId());
    } catch (RestClientException e) {
      boolean notFound =
          e.getErrorCode() == SUBJECT_NOT_FOUND || e.getErrorCode() == VERSION_NOT_FOUND;
      throw new SchemaResolutionException(
          "Cannot read " + what + " from the Schema Registry: " + e.getMessage(), notFound, e);
    } catch (IOException e) {
      throw new SchemaResolutionException(
          "Cannot reach the Schema Registry to read " + what + ": " + e.getMessage(), false, e);
    }
    if (!(parsed instanceof ProtobufSchema)) {
      throw new SchemaResolutionException(
          "Schema of " + what + " did not parse as a protobuf schema", false, null);
    }
    ProtobufSchema schema = (ProtobufSchema) parsed;
    Descriptor descriptor = selectMessage(schema, messageName, what);
    return new ResolvedProtoSchema(
        subject, metadata.getVersion(), metadata.getId(), schema, descriptor);
  }

  private static Descriptor selectMessage(
      ProtobufSchema schema, @Nullable String messageName, String what) {
    if (messageName == null) {
      return schema.toDescriptor();
    }
    Descriptor descriptor;
    try {
      descriptor = schema.toDescriptor(messageName);
    } catch (IllegalArgumentException e) {
      descriptor = null;
    }
    if (descriptor == null) {
      String available =
          schema.toDescriptor().getFile().getMessageTypes().stream()
              .map(Descriptor::getFullName)
              .collect(Collectors.joining(", "));
      throw new SchemaResolutionException(
          "Message '" + messageName + "' is not in the schema of " + what + "; it has: " + available,
          true,
          null);
    }
    return descriptor;
  }
}
