package com.yahoo.ycsb.db.hashmap;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Basic DB that just serve requests via HashMap, instead of doing them against a database.
 */
public final class HashMapDB extends DB {
  // Table -> Row -> Column -> Data
  private ConcurrentMap<String, ConcurrentNavigableMap<String, Map<String, byte[]>>> db;

  @Override
  public void init() throws DBException {
    db = new ConcurrentHashMap<>();
  }

  @Override
  public void cleanup() throws DBException {
    db = null;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      ConcurrentNavigableMap<String, Map<String, byte[]>> dbTable = getTable(table);
      Map<String, byte[]> dbRow = dbTable.get(key);

      if (dbRow == null) {
        return Status.NOT_FOUND;
      } else {
        if (fields == null) {
          dbRow.forEach((k, v) -> result.put(k, new ByteArrayByteIterator(v)));
        } else {
          for (String f : fields) {
            result.put(f, new ByteArrayByteIterator(dbRow.get(f)));
          }
        }

        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error doing read: " + e);
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
    try {
      ConcurrentNavigableMap<String, Map<String, byte[]>> dbTable = getTable(table);
      for (Map<String, byte[]> row : dbTable.tailMap(startkey).values()) {
        recordcount--;
        if (recordcount < 0) {
          break;
        }

        HashMap<String, ByteIterator> rowResult = new HashMap<>();
        if (fields == null) {
          row.forEach((k, v) -> rowResult.put(k, new ByteArrayByteIterator(v)));
        } else {
          for (String field : fields) {
            rowResult.put(field, new ByteArrayByteIterator(row.get(field)));
          }
        }
        result.add(rowResult);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error doing scan: " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return doUpdate(table, key, values, false);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return doUpdate(table, key, values, true);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      ConcurrentNavigableMap<String, Map<String, byte[]>> dbTable = getTable(table);
      if (dbTable.remove(key) == null) {
        return Status.NOT_FOUND;
      } else {
        return Status.OK;
      }
    } catch (Exception e) {
      System.err.println("Error doing delete: " + e);
      return Status.ERROR;
    }
  }

  /**
   * Get an existing table or create an empty table.
   *
   * @param tableName the name of table
   * @return the existing table or the empty table
   */
  ConcurrentNavigableMap<String, Map<String, byte[]>> getTable(String tableName) {
    return db.computeIfAbsent(tableName, ignored -> new ConcurrentSkipListMap<>());
  }

  /**
   * Get an existing row or create an empty row in a table.
   *
   * @param table  the table
   * @param rowKey the row key in the table
   * @return the existing row or the empty row
   */
  private Map<String, byte[]> getRow(
      ConcurrentNavigableMap<String, Map<String, byte[]>> table,
      String rowKey) {
    return table.computeIfAbsent(rowKey, ignored -> new HashMap<>());
  }

  private Status doUpdate(String table, String key, Map<String, ByteIterator> values, boolean isInsert) {
    try {
      ConcurrentNavigableMap<String, Map<String, byte[]>> dbTable = getTable(table);
      dbTable.compute(key, (rowKey, row) -> {
          Map<String, byte[]> newRow = new HashMap<>();
          if (isInsert) {
            values.forEach((f, v) -> newRow.put(f, v.toArray()));
          } else {
            assert row != null;
            row.forEach((f, v) -> {
                if (values.containsKey(f)) {
                  newRow.put(f, values.get(f).toArray());
                } else {
                  newRow.put(f, v);
                }
              });
          }
          return newRow;
        });

      return Status.OK;
    } catch (Exception e) {
      System.err.println("Error doing update: " + e);
      return Status.ERROR;
    }
  }
}
