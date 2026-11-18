package com.bbrownsound.flink.formats.proto.registry.confluent.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bbrownsound.flink.formats.proto.test.v1.TestSimple;
import org.junit.jupiter.api.Test;

class ProtoDescriptorsTest {

  /** Looks like a message class, but its getDescriptor() fails. */
  public static final class FailingDescriptor {
    public static Object getDescriptor() {
      throw new IllegalStateException("boom");
    }
  }

  /** Looks like a message class, but its getDescriptor() returns something else. */
  public static final class WrongDescriptor {
    public static Object getDescriptor() {
      return "not a descriptor";
    }
  }

  @Test
  void returnsTheDescriptorOfAMessageClass() {
    assertSame(
        TestSimple.SimpleMessage.getDescriptor(),
        ProtoDescriptors.descriptorOf(TestSimple.SimpleMessage.class));
  }

  @Test
  void rejectsAClassWithoutGetDescriptor() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> ProtoDescriptors.descriptorOf(String.class));
    assertEquals("Not a protobuf message class (no getDescriptor()): java.lang.String", e.getMessage());
  }

  @Test
  void reportsAFailingGetDescriptor() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ProtoDescriptors.descriptorOf(FailingDescriptor.class));
    assertEquals("Failed to get descriptor for " + FailingDescriptor.class.getName(), e.getMessage());
    assertEquals("boom", e.getCause().getCause().getMessage());
  }

  @Test
  void rejectsAGetDescriptorThatReturnsSomethingElse() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> ProtoDescriptors.descriptorOf(WrongDescriptor.class));
    assertEquals(
        "getDescriptor() did not return a Descriptor: " + WrongDescriptor.class.getName(),
        e.getMessage());
  }
}
