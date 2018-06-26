package com.yahoo.ycsb.db.kv;

import com.google.common.collect.ImmutableMap;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import org.apache.commons.lang3.SerializationUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Simple Key-Value Store.
 */
public class SimpleKVStoreDBClient extends DB {
  private ISimpleKVStoreEngine databaseEngine;
  private String tableKeySeparator;

  @Override
  public void init() throws DBException {
    final String databaseEngineClassString = getProperties().getProperty("simplekv.database-engine.class");
    System.err.println("simplekv.database-engine.class = " + databaseEngineClassString);
    try {
      final Class<ISimpleKVStoreEngine> databaseEngineClass =
          (Class<ISimpleKVStoreEngine>) Class.forName(databaseEngineClassString);
      databaseEngine = databaseEngineClass.newInstance();
      databaseEngine.init(getProperties());
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new DBException(e);
    } catch (Exception e) {
      throw new DBException(e);
    }

    tableKeySeparator = getProperties().getProperty("simplekv.table-key-separator", "#");
  }

  @Override
  public void cleanup() throws DBException {
    databaseEngine = null;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    String realKey = getRealKey(table, key);

    try {
      byte[] rowBytes = databaseEngine.get(realKey);
      ImmutableMap<String, byte[]> row = SerializationUtils.deserialize(rowBytes);

      if (fields == null) {
        row.forEach((k, v) -> {
            result.put(k, new ByteArrayByteIterator(v));
          });
      } else {
        fields.forEach(k -> {
            result.put(k, new ByteArrayByteIterator(row.get(k)));
          });
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(
      String table,
      String startkey,
      int recordcount,
      Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String realKey = getRealKey(table, key);

    try {
      byte[] rowBytes = databaseEngine.get(realKey);
      ImmutableMap<String, byte[]> row = SerializationUtils.deserialize(rowBytes);

      ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builderWithExpectedSize(row.size());
      row.forEach((k, v) -> {
          if (values.containsKey(k)) {
            builder.put(k, values.get(k).toArray());
          } else {
            builder.put(k, v);
          }
        });
      byte[] realValue = SerializationUtils.serialize(builder.build());

      databaseEngine.put(realKey, realValue);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    String realKey = getRealKey(table, key);

    ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builderWithExpectedSize(values.size());
    values.forEach((k, v) -> builder.put(k, v.toArray()));
    byte[] realValue = SerializationUtils.serialize(builder.build());

    try {
      databaseEngine.put(realKey, realValue);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {
    try {
      databaseEngine.delete(getRealKey(table, key));
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  private String getRealKey(String table, String key) {
    return table + tableKeySeparator + key;
  }
}
