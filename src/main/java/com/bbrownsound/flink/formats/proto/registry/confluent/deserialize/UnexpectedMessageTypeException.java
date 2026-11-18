package com.bbrownsound.flink.formats.proto.registry.confluent.deserialize;

import java.io.IOException;

/**
 * A record's writer message is not the message the source expects ({@code message-name}, or the
 * message of {@code message-class}). Handled like any other deserialize failure, so it follows
 * {@code on-deserialize-error}: it fails the task by default, and with {@code skip} the record is
 * dropped, counted in {@code numDeserializeErrors} and sent to the dead-letter topic if one is set.
 */
public class UnexpectedMessageTypeException extends IOException {
  private static final long serialVersionUID = 1L;

  /**
   * Creates the exception.
   *
   * @param topic the topic the record was read from
   * @param schemaId the writer schema ID from the record's wire header
   * @param actual the full name of the record's writer message
   * @param expected the full name the source expects
   */
  public UnexpectedMessageTypeException(
      String topic, int schemaId, String actual, String expected) {
    super(
        "record on topic '"
            + topic
            + "' is a '"
            + actual
            + "' (writer schema id "
            + schemaId
            + ") but the source expects '"
            + expected
            + "'");
  }
}
