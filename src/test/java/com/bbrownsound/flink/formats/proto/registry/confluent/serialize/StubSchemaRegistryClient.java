package com.bbrownsound.flink.formats.proto.registry.confluent.serialize;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * In-memory Schema Registry for sink tests. Behaves like {@link MockSchemaRegistryClient} (which
 * answers a missing subject exactly like a real registry: HTTP 404 with error code 40401) and adds
 * what the tests need to assert on calls: a counter for latest-schema lookups, a counter for
 * registration attempts, and a way to make the lookup fail.
 */
final class StubSchemaRegistryClient extends MockSchemaRegistryClient {

  private int latestLookups;
  private int registerCalls;
  private boolean counting = true;
  private boolean unparseable;
  private ParsedSchema parsedOverride;
  private Exception lookupFailure;
  private Runnable onLookup = () -> {};

  StubSchemaRegistryClient() {
    super(Collections.singletonList(new ProtobufSchemaProvider()));
  }

  /** Registers a schema the way an operator would, without counting it as a sink registration. */
  int seed(String subject, ParsedSchema schema) throws IOException, RestClientException {
    counting = false;
    try {
      return super.register(subject, schema);
    } finally {
      counting = true;
    }
  }

  /** Makes every following latest-schema lookup throw the given exception; {@code null} heals. */
  void failLookupsWith(Exception failure) {
    if (failure != null
        && !(failure instanceof IOException)
        && !(failure instanceof RestClientException)
        && !(failure instanceof RuntimeException)) {
      throw new IllegalArgumentException("Not an exception the client can throw: " + failure);
    }
    this.lookupFailure = failure;
  }

  /** Runs the action at the start of every latest-schema lookup, e.g. to let time pass in it. */
  void onLookup(Runnable action) {
    this.onLookup = action;
  }

  /** Makes the registry return schemas no provider can parse, like a non-Protobuf subject. */
  void returnUnparseableSchemas() {
    this.unparseable = true;
  }

  /** Makes the registry's schemas parse to the given schema, e.g. one that is not Protobuf. */
  void parseSchemasAs(ParsedSchema parsed) {
    this.parsedOverride = parsed;
  }

  int latestLookups() {
    return latestLookups;
  }

  int registerCalls() {
    return registerCalls;
  }

  /** Versions registered under the subject; empty when the subject does not exist. */
  List<Integer> versions(String subject) throws IOException, RestClientException {
    try {
      return getAllVersions(subject);
    } catch (RestClientException e) {
      if (e.getErrorCode() == RowDataProtoSerializer.SUBJECT_NOT_FOUND_ERROR_CODE) {
        return Collections.emptyList();
      }
      throw e;
    }
  }

  @Override
  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException {
    latestLookups++;
    onLookup.run();
    if (lookupFailure instanceof IOException) {
      throw (IOException) lookupFailure;
    }
    if (lookupFailure instanceof RestClientException) {
      throw (RestClientException) lookupFailure;
    }
    if (lookupFailure instanceof RuntimeException) {
      throw (RuntimeException) lookupFailure;
    }
    return super.getLatestSchemaMetadata(subject);
  }

  @Override
  public Optional<ParsedSchema> parseSchema(Schema schema) {
    if (unparseable) {
      return Optional.empty();
    }
    return parsedOverride != null ? Optional.of(parsedOverride) : super.parseSchema(schema);
  }

  @Override
  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException {
    countRegistration();
    return super.register(subject, schema);
  }

  @Override
  public int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    countRegistration();
    return super.register(subject, schema, normalize);
  }

  @Override
  public int register(String subject, ParsedSchema schema, int version, int id)
      throws IOException, RestClientException {
    countRegistration();
    return super.register(subject, schema, version, id);
  }

  @Override
  public RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags)
      throws RestClientException {
    countRegistration();
    return super.registerWithResponse(subject, schema, normalize, propagateSchemaTags);
  }

  private void countRegistration() {
    if (counting) {
      registerCalls++;
    }
  }
}
