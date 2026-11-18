package com.bbrownsound.flink.formats.proto.registry.confluent.cli;

/** Thrown for invalid command line arguments; the CLI prints the message and the usage. */
final class UsageException extends Exception {

  private static final long serialVersionUID = 1L;

  UsageException(String message) {
    super(message);
  }
}
