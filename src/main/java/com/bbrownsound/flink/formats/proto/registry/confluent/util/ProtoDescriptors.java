package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import com.google.protobuf.Descriptors.Descriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Descriptor lookups that do not depend on Flink. */
public final class ProtoDescriptors {

  private ProtoDescriptors() {}

  /**
   * Returns the descriptor of a generated protobuf message class, via its static {@code
   * getDescriptor()} method.
   *
   * @param messageClass a generated protobuf message class
   * @return the message descriptor
   * @throws IllegalArgumentException if the class is not a generated protobuf message class
   */
  public static Descriptor descriptorOf(Class<?> messageClass) {
    Object result;
    try {
      Method getDescriptor = messageClass.getMethod("getDescriptor");
      result = getDescriptor.invoke(null);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          "Not a protobuf message class (no getDescriptor()): " + messageClass.getName(), e);
    } catch (IllegalAccessException | InvocationTargetException | RuntimeException e) {
      throw new IllegalArgumentException(
          "Failed to get descriptor for " + messageClass.getName(), e);
    }
    if (result instanceof Descriptor) {
      return (Descriptor) result;
    }
    throw new IllegalArgumentException(
        "getDescriptor() did not return a Descriptor: " + messageClass.getName());
  }
}
