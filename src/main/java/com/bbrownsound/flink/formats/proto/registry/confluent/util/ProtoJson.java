package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

/**
 * Converts a Protobuf message to and from its canonical proto3 JSON form, for message fields a
 * table declares as {@code STRING} ({@code google.protobuf.Struct}, {@code Value} and {@code
 * ListValue} by default). Field names are the Protobuf field names; fields left at their default
 * value are omitted, and the output has no insignificant whitespace.
 */
public final class ProtoJson {

  private static final JsonFormat.Printer PRINTER =
      JsonFormat.printer().preservingProtoFieldNames().omittingInsignificantWhitespace();
  private static final JsonFormat.Parser PARSER = JsonFormat.parser();

  private ProtoJson() {}

  /**
   * Prints a message as JSON.
   *
   * @param message the message
   * @return the message's JSON
   * @throws InvalidProtocolBufferException if the message cannot be printed, e.g. a {@code
   *     google.protobuf.Any} whose type is unknown
   */
  public static String toJson(MessageOrBuilder message) throws InvalidProtocolBufferException {
    return PRINTER.print(message);
  }

  /**
   * Parses JSON into a message of the given type.
   *
   * @param json the JSON text
   * @param type the message type
   * @return the parsed message
   * @throws IllegalArgumentException if the text is not valid JSON for the type
   */
  public static Message fromJson(String json, Descriptor type) {
    final DynamicMessage.Builder builder = DynamicMessage.newBuilder(type);
    try {
      PARSER.merge(json, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Cannot write a STRING column to a "
              + type.getFullName()
              + " field: it is not valid JSON for that message type. "
              + e.getMessage(),
          e);
    }
    return builder.build();
  }
}
