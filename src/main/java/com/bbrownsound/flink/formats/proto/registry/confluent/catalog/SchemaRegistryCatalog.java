package com.bbrownsound.flink.formats.proto.registry.confluent.catalog;

import com.bbrownsound.flink.formats.proto.registry.confluent.schema.ResolvedProtoSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistryClients;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaRegistrySchemaResolver;
import com.bbrownsound.flink.formats.proto.registry.confluent.schema.SchemaResolutionException;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

/**
 * A read-only catalog that exposes every Kafka topic with a protobuf schema in the Confluent Schema
 * Registry as a table, so the topic can be queried without a {@code CREATE TABLE}.
 *
 * <p>Subjects are read with the {@code TopicNameStrategy}: a {@code <topic>-value} subject whose
 * latest schema is protobuf becomes table {@code <topic>} in the catalog's only database. If
 * {@code <topic>-key} also holds a protobuf schema, the key message's fields become columns too.
 * Each table is a Kafka connector table using the {@code proto-confluent} format; the catalog's
 * Kafka options ({@code properties.*}, {@code scan.*}, {@code sink.*}) are set on every table and
 * can be overridden per query with {@code /*+ OPTIONS(...) *}{@code /} hints.
 *
 * <p>The schema is resolved again on every {@link #getTable(ObjectPath)}, so a query always sees
 * the latest registered version. Creating, altering or dropping anything is rejected.
 */
public class SchemaRegistryCatalog extends AbstractCatalog {

  static final String VALUE_SUFFIX = "-value";
  static final String KEY_SUFFIX = "-key";

  private static final int SUBJECT_NOT_FOUND = 40401;
  private static final int VERSION_NOT_FOUND = 40402;

  private final SchemaRegistryCatalogConfig config;
  private final Function<SchemaRegistryCatalogConfig, SchemaRegistryClient> clientFactory;
  @Nullable
  private SchemaRegistrySchemaResolver resolver;

  /**
   * Creates a catalog that connects to the registry named in the config when it is opened.
   *
   * @param name the catalog name
   * @param config the catalog settings
   */
  public SchemaRegistryCatalog(String name, SchemaRegistryCatalogConfig config) {
    this(
        name,
        config,
        c -> SchemaRegistryClients.create(c.getUrl(), c.getClientProperties()));
  }

  /**
   * Creates a catalog over registry clients from the given factory, called once per {@link
   * #open()}. The clients must have the protobuf schema provider registered.
   *
   * @param name the catalog name
   * @param config the catalog settings
   * @param clientFactory creates the registry client
   */
  public SchemaRegistryCatalog(
      String name,
      SchemaRegistryCatalogConfig config,
      Function<SchemaRegistryCatalogConfig, SchemaRegistryClient> clientFactory) {
    super(name, Objects.requireNonNull(config, "config").getDefaultDatabase());
    this.config = config;
    this.clientFactory = Objects.requireNonNull(clientFactory, "clientFactory");
  }

  @Override
  public void open() throws CatalogException {
    if (resolver == null) {
      resolver = new SchemaRegistrySchemaResolver(clientFactory.apply(config));
    }
  }

  @Override
  public void close() throws CatalogException {
    if (resolver != null) {
      SchemaRegistryClient client = resolver.getClient();
      resolver = null;
      try {
        client.close();
      } catch (IOException e) {
        throw new CatalogException("Cannot close the Schema Registry client", e);
      }
    }
  }

  private SchemaRegistrySchemaResolver resolver() {
    if (resolver == null) {
      throw new CatalogException("Catalog '" + getName() + "' is not open.");
    }
    return resolver;
  }

  // ------------------------------------------------------------------ databases

  @Override
  public List<String> listDatabases() {
    return Collections.singletonList(getDefaultDatabase());
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
    return new RegistryDatabase("Kafka topics with a protobuf schema in the Schema Registry");
  }

  @Override
  public boolean databaseExists(String databaseName) {
    return getDefaultDatabase().equals(databaseName);
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists) {
    throw readOnly("create database '" + name + "'");
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade) {
    throw readOnly("drop database '" + name + "'");
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists) {
    throw readOnly("alter database '" + name + "'");
  }

  // ------------------------------------------------------------------ tables

  /**
   * Lists the topics whose {@code <topic>-value} subject's latest schema is protobuf, sorted by
   * name. Asks the registry for the latest version of every {@code -value} subject.
   */
  @Override
  public List<String> listTables(String databaseName) throws DatabaseNotExistException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
    List<String> tables = new ArrayList<>();
    for (String subject : allSubjects()) {
      if (subject.endsWith(VALUE_SUFFIX)
          && subject.length() > VALUE_SUFFIX.length()
          && latestProtobufVersion(subject).isPresent()) {
        tables.add(subject.substring(0, subject.length() - VALUE_SUFFIX.length()));
      }
    }
    Collections.sort(tables);
    return tables;
  }

  @Override
  public List<String> listViews(String databaseName) throws DatabaseNotExistException {
    if (!databaseExists(databaseName)) {
      throw new DatabaseNotExistException(getName(), databaseName);
    }
    return Collections.emptyList();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException {
    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new TableNotExistException(getName(), tablePath);
    }
    String topic = tablePath.getObjectName();
    String valueSubject = topic + VALUE_SUFFIX;
    SchemaMetadata valueVersion =
        latestProtobufVersion(valueSubject)
            .orElseThrow(() -> new TableNotExistException(getName(), tablePath));
    ResolvedProtoSchema value = resolve(valueSubject, valueVersion, tablePath);
    ResolvedProtoSchema key = null;
    if (config.isKeyEnabled()) {
      String keySubject = topic + KEY_SUFFIX;
      Optional<SchemaMetadata> keyVersion = latestProtobufVersion(keySubject);
      if (keyVersion.isPresent()) {
        key = resolve(keySubject, keyVersion.get(), tablePath);
      }
    }
    return KafkaTables.create(topic, value, key, config);
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) {
    return databaseExists(tablePath.getDatabaseName())
        && latestProtobufVersion(tablePath.getObjectName() + VALUE_SUFFIX).isPresent();
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists) {
    throw readOnly("drop table " + tablePath.getFullName());
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists) {
    throw readOnly("rename table " + tablePath.getFullName());
  }

  @Override
  public void createTable(
      ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) {
    throw readOnly(
        "create table "
            + tablePath.getFullName()
            + " (register a protobuf schema under subject '"
            + tablePath.getObjectName()
            + VALUE_SUFFIX
            + "' instead, or create the table in another catalog)");
  }

  @Override
  public void alterTable(
      ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists) {
    throw readOnly("alter table " + tablePath.getFullName());
  }

  // ------------------------------------------------------------------ partitions

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return Collections.emptyList();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters) {
    return Collections.emptyList();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws PartitionNotExistException {
    throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return false;
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists) {
    throw readOnly("create a partition of " + tablePath.getFullName());
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists) {
    throw readOnly("drop a partition of " + tablePath.getFullName());
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists) {
    throw readOnly("alter a partition of " + tablePath.getFullName());
  }

  // ------------------------------------------------------------------ functions

  @Override
  public List<String> listFunctions(String dbName) throws DatabaseNotExistException {
    if (!databaseExists(dbName)) {
      throw new DatabaseNotExistException(getName(), dbName);
    }
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists) {
    throw readOnly("create function " + functionPath.getFullName());
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists) {
    throw readOnly("alter function " + functionPath.getFullName());
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists) {
    throw readOnly("drop function " + functionPath.getFullName());
  }

  // ------------------------------------------------------------------ statistics

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists) {
    throw readOnly("alter statistics of " + tablePath.getFullName());
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) {
    throw readOnly("alter statistics of " + tablePath.getFullName());
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists) {
    throw readOnly("alter statistics of " + tablePath.getFullName());
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists) {
    throw readOnly("alter statistics of " + tablePath.getFullName());
  }

  // ------------------------------------------------------------------ registry access

  private List<String> allSubjects() {
    try {
      return new ArrayList<>(resolver().getClient().getAllSubjects());
    } catch (IOException | RestClientException e) {
      throw new CatalogException(
          "Cannot list subjects in the Schema Registry at " + config.getUrl() + ": " + e, e);
    }
  }

  /**
   * Returns the latest version of a subject if its schema is protobuf, or empty if the subject does
   * not exist or holds another schema type.
   */
  private Optional<SchemaMetadata> latestProtobufVersion(String subject) {
    SchemaMetadata metadata;
    try {
      metadata = resolver().getClient().getLatestSchemaMetadata(subject);
    } catch (RestClientException e) {
      if (e.getErrorCode() == SUBJECT_NOT_FOUND || e.getErrorCode() == VERSION_NOT_FOUND) {
        return Optional.empty();
      }
      throw new CatalogException(
          "Cannot read subject '" + subject + "' from the Schema Registry: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new CatalogException(
          "Cannot reach the Schema Registry to read subject '" + subject + "': " + e.getMessage(),
          e);
    }
    return ProtobufSchema.TYPE.equals(metadata.getSchemaType())
        ? Optional.of(metadata)
        : Optional.empty();
  }

  private ResolvedProtoSchema resolve(String subject, SchemaMetadata version, ObjectPath tablePath)
      throws TableNotExistException {
    try {
      return resolver().resolve(subject, version.getVersion(), null);
    } catch (SchemaResolutionException e) {
      if (e.isNotFound()) {
        // Deleted between the lookup of the latest version and reading it.
        throw new TableNotExistException(getName(), tablePath, e);
      }
      throw new CatalogException(
          "Cannot read table " + tablePath.getFullName() + ": " + e.getMessage(), e);
    }
  }

  private UnsupportedOperationException readOnly(String what) {
    return new UnsupportedOperationException(
        "Catalog '"
            + getName()
            + "' is a read-only view of the Schema Registry; cannot "
            + what
            + ".");
  }

  /** The catalog's only database. */
  private static final class RegistryDatabase implements CatalogDatabase {

    private final String comment;

    RegistryDatabase(String comment) {
      this.comment = comment;
    }

    @Override
    public Map<String, String> getProperties() {
      return Collections.emptyMap();
    }

    @Override
    public String getComment() {
      return comment;
    }

    @Override
    public CatalogDatabase copy() {
      return this;
    }

    @Override
    public CatalogDatabase copy(Map<String, String> properties) {
      return this;
    }

    @Override
    public Optional<String> getDescription() {
      return Optional.of(comment);
    }

    @Override
    public Optional<String> getDetailedDescription() {
      return Optional.of(comment);
    }
  }
}
