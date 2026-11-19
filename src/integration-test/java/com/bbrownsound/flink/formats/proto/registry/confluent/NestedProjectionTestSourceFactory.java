package com.bbrownsound.flink.formats.proto.registry.confluent;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

/**
 * A test connector that pushes nested projections into its format, which neither the filesystem
 * nor the Kafka connector does (GitHub issue #217): {@code supportsNestedProjection()} is whatever
 * the format answers, and the projection the planner pushes, nested paths included, is handed to
 * {@link ProjectableDecodingFormat#createRuntimeDecoder(DynamicTableSource.Context, DataType,
 * int[][])}. It reads the Confluent wire-format records registered under its {@code records} key
 * and emits the decoded rows, bounded.
 */
public class NestedProjectionTestSourceFactory implements DynamicTableSourceFactory {

  static final String IDENTIFIER = "proto-confluent-nested-projection-test";

  static final ConfigOption<String> RECORDS =
      ConfigOptions.key("records").stringType().noDefaultValue();

  private static final Map<String, List<byte[]>> REGISTERED = new ConcurrentHashMap<>();

  /**
   * Registers the records a table with {@code 'records' = key} reads.
   *
   * @param key the value of the table's {@code records} option
   * @param records Confluent wire-format records
   */
  static void register(String key, List<byte[]> records) {
    REGISTERED.put(key, records);
  }

  @Override
  public String factoryIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Set.of(RECORDS, FactoryUtil.FORMAT);
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Set.of();
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
    DecodingFormat<DeserializationSchema<RowData>> format =
        helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
    helper.validate();
    return new Source(
        format,
        context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType(),
        helper.getOptions().get(RECORDS),
        null);
  }

  /** The source: decodes the registered records with the format's (projected) decoder. */
  private static final class Source implements ScanTableSource, SupportsProjectionPushDown {
    private final DecodingFormat<DeserializationSchema<RowData>> format;
    private final DataType physicalDataType;
    private final String recordsKey;
    private int[][] projection;

    Source(
        DecodingFormat<DeserializationSchema<RowData>> format,
        DataType physicalDataType,
        String recordsKey,
        int[][] projection) {
      this.format = format;
      this.physicalDataType = physicalDataType;
      this.recordsKey = recordsKey;
      this.projection = projection;
    }

    @Override
    public boolean supportsNestedProjection() {
      return format instanceof ProjectableDecodingFormat
          && ((ProjectableDecodingFormat<?>) format).supportsNestedProjection();
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
      this.projection = projectedFields;
    }

    @Override
    public ChangelogMode getChangelogMode() {
      return format.getChangelogMode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
      final DeserializationSchema<RowData> decoder =
          projection == null
              ? format.createRuntimeDecoder(context, physicalDataType)
              : ((ProjectableDecodingFormat<DeserializationSchema<RowData>>) format)
                  .createRuntimeDecoder(context, physicalDataType, projection);
      final List<byte[]> records = REGISTERED.getOrDefault(recordsKey, List.of());
      return new DataStreamScanProvider() {
        @Override
        public DataStream<RowData> produceDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment env) {
          return env.fromData(records, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
              .setParallelism(1)
              .map(new Decode(decoder))
              .returns(decoder.getProducedType())
              .setParallelism(1);
        }

        @Override
        public boolean isBounded() {
          return true;
        }
      };
    }

    @Override
    public DynamicTableSource copy() {
      return new Source(format, physicalDataType, recordsKey, projection);
    }

    @Override
    public String asSummaryString() {
      return IDENTIFIER;
    }
  }

  /** Decodes one record with the format's decoder, opened once per task. */
  private static final class Decode extends RichMapFunction<byte[], RowData> {
    private static final long serialVersionUID = 1L;
    private final DeserializationSchema<RowData> decoder;

    Decode(DeserializationSchema<RowData> decoder) {
      this.decoder = decoder;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
      decoder.open(null);
    }

    @Override
    public RowData map(byte[] value) throws Exception {
      return decoder.deserialize(value);
    }
  }
}
