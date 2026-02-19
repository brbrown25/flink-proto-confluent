package com.bbrownsound.flink.formats.proto.registry.confluent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bbrownsound.flink.formats.proto.registry.confluent.config.ProtoConfluentFormatConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.junit.jupiter.api.Test;

class ProtoDecodingFormatTest {

  @Test
  void getChangelogMode() {
    ProtoConfluentFormatConfig config =
        new ProtoConfluentFormatConfig("http://localhost:8081", "t", false, java.util.Map.of());
    ProtoDecodingFormat format = new ProtoDecodingFormat(config);
    assertEquals(ChangelogMode.insertOnly(), format.getChangelogMode());
  }
}
