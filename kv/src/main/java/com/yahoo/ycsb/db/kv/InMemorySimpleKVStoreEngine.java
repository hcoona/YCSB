package com.yahoo.ycsb.db.kv;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The in-memory implementation of a simple key-value store.
 */
public class InMemorySimpleKVStoreEngine implements ISimpleKVStoreEngine {
  private final Map<String, byte[]> store = new ConcurrentSkipListMap<>();

  @Override
  public void init(Properties properties) throws Exception {
    store.clear();
  }

  @Override
  public void cleanup() {
    store.clear();
  }

  @Override
  public byte[] get(String key) throws Exception {
    return store.get(key);
  }

  @Override
  public void put(String key, byte[] value) throws Exception {
    store.put(key, value);
  }

  @Override
  public void delete(String key) throws Exception {
    store.remove(key);
  }
}
