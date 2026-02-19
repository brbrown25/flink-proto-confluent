package com.bbrownsound.flink.formats.proto.registry.confluent;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import com.bbrownsound.flink.formats.proto.registry.confluent.serialize.ProtoRowDataSerializationSchema;
import com.bbrownsound.flink.formats.proto.registry.confluent.util.ProtoToLogicalType;
import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import java.util.Map;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class RowToProtoTest extends ProtoConfluentTestBase {

  private KafkaProtobufDeserializer<DynamicMessage> deserializer() {
    return new KafkaProtobufDeserializer<>(getSchemaRegistryClient(), getConfig());
  }

  @Test
  public void testRowToProtoWithSchemaRegistered() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var serializer =
        new ProtoRowDataSerializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor()),
            new ProtoConfluentFormatConfig(url, "simple_test", false, config));
    serializer.open(null);

    serializeMessage(TestSimple.SimpleMessage.newBuilder().build(), "simple_test");

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("field1"));
    row.setField(1, StringData.fromString("field2"));

    byte[] serialized = serializer.serialize(row);
    DynamicMessage message = deserializer().deserialize("simple_test", serialized);
    Assertions.assertEquals(
        "field1", message.getField(message.getDescriptorForType().findFieldByName("content")));
    Assertions.assertEquals(
        "field2", message.getField(message.getDescriptorForType().findFieldByName("date_time")));
  }

  @Test
  public void testRowToProtoNoSchemaRegistered() throws Exception {
    String url = getSchemaRegistryUrl();
    Map<String, String> config = getConfig();
    var serializer =
        new ProtoRowDataSerializationSchema(
            (RowType) ProtoToLogicalType.toLogicalType(TestSimple.SimpleMessage.getDescriptor()),
            new ProtoConfluentFormatConfig(url, "simple_test_not_registered", false, config));
    serializer.open(null);

    serializeMessage(TestSimple.SimpleMessage.newBuilder().build(), "simple_test_not_registered");

    GenericRowData row = new GenericRowData(2);
    row.setField(0, StringData.fromString("field1"));
    row.setField(1, StringData.fromString("field2"));

    byte[] serialized = serializer.serialize(row);
    DynamicMessage message = deserializer().deserialize("simple_test_not_registered", serialized);
    Assertions.assertEquals(
        "field1", message.getField(message.getDescriptorForType().findFieldByName("content")));
    Assertions.assertEquals(
        "field2", message.getField(message.getDescriptorForType().findFieldByName("date_time")));
  }
}
