package com.bbrownsound.flink.formats.proto.registry.confluent.bench;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Memoizes the schema/ID lookups a serializer or deserializer makes on every record, the way {@code
 * CachedSchemaRegistryClient} does with its schema-to-ID and ID-to-schema caches.
 *
 * <p>{@code MockSchemaRegistryClient} answers those lookups by recomputing them: its {@code
 * getIdWithResponse} normalizes the schema on every call, which costs tens of microseconds and is
 * work the production client never repeats. Without this wrapper the serialize benchmark would
 * mostly measure the mock.
 */
final class CachingRegistryClient implements InvocationHandler {

  private final SchemaRegistryClient delegate;
  private final Map<List<Object>, Object> answers = new ConcurrentHashMap<>();

  private CachingRegistryClient(SchemaRegistryClient delegate) {
    this.delegate = delegate;
  }

  /**
   * Wraps a client.
   *
   * @param delegate the client that answers the first lookup of each key
   * @return a client that answers repeated ID and schema lookups from memory
   */
  static SchemaRegistryClient wrap(SchemaRegistryClient delegate) {
    return (SchemaRegistryClient)
        Proxy.newProxyInstance(
            SchemaRegistryClient.class.getClassLoader(),
            new Class<?>[] {SchemaRegistryClient.class},
            new CachingRegistryClient(delegate));
  }

  private static boolean isCachedLookup(Method method) {
    String name = method.getName();
    return name.startsWith("getId") || name.startsWith("getSchemaBy");
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (!isCachedLookup(method)) {
      return call(method, args);
    }
    List<Object> key = Arrays.asList(method, Arrays.asList(nonNull(args)));
    Object cached = answers.get(key);
    if (cached == null) {
      cached = call(method, args);
      if (cached != null) {
        answers.put(key, cached);
      }
    }
    return cached;
  }

  private static Object[] nonNull(Object[] args) {
    return args == null ? new Object[0] : args;
  }

  private Object call(Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }
}
