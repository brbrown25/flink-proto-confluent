package com.bbrownsound.flink.formats.proto.registry.confluent.schema;

/** Thrown when a schema cannot be resolved from the Schema Registry. */
public class SchemaResolutionException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final boolean notFound;

  /**
   * Creates the exception.
   *
   * @param message the message
   * @param notFound whether the subject, version or message does not exist (as opposed to the
   *     registry being unreachable or the schema being unusable)
   * @param cause the cause, or {@code null}
   */
  public SchemaResolutionException(String message, boolean notFound, Throwable cause) {
    super(message, cause);
    this.notFound = notFound;
  }

  /**
   * Returns whether the subject, version or message does not exist. A catalog can map this to
   * "table does not exist" and everything else to a catalog error.
   */
  public boolean isNotFound() {
    return notFound;
  }
}
