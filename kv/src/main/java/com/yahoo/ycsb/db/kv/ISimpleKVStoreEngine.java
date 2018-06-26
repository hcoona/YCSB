package com.yahoo.ycsb.db.kv;

import java.util.Properties;

/**
 * The interface of real key-value store engine.
 */
public interface ISimpleKVStoreEngine {
  void init(Properties properties) throws Exception;

  void cleanup();

  byte[] get(String key) throws Exception;

  void put(String key, byte[] value) throws Exception;

  void delete(String key) throws Exception;
}
