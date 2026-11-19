package com.bbrownsound.flink.formats.proto.registry.confluent.catalog;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * Creates a {@link SchemaRegistryCatalog} for {@code CREATE CATALOG ... WITH ('type' =
 * 'proto-confluent', ...)}.
 *
 * <p>Shares its identifier with the {@code proto-confluent} format factory. Flink looks factories up
 * by kind first (catalog, format, connector) and only then by identifier, so the two never compete.
 */
public class SchemaRegistryCatalogFactory implements CatalogFactory {

  /** Catalog type for {@code 'type' = 'proto-confluent'}. */
  public static final String IDENTIFIER = "proto-confluent";

  /** Creates the factory; invoked by the Flink service loader. */
  public SchemaRegistryCatalogFactory() {}

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(SchemaRegistryCatalogOptions.URL);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(SchemaRegistryCatalogOptions.DEFAULT_DATABASE);
    options.add(SchemaRegistryCatalogOptions.KEY_ENABLED);
    options.add(SchemaRegistryCatalogOptions.KEY_FIELDS_PREFIX);
    options.add(SchemaRegistryCatalogOptions.RECURSIVE_MESSAGE);
    options.addAll(SchemaRegistryCatalogOptions.CONNECTION_OPTIONS);
    return options;
  }

  @Override
  public Catalog createCatalog(Context context) {
    FactoryUtil.CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);
    helper.validateExcept(SchemaRegistryCatalogOptions.passThroughPrefixes());
    return new SchemaRegistryCatalog(
        context.getName(), SchemaRegistryCatalogConfig.fromOptions(context.getOptions()));
  }
}
