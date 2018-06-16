package com.yahoo.ycsb.db.hashmap;


import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;

import static org.junit.Assert.assertEquals;

/**
 * Test HashMapDB.
 */
public class HashMapDbTest {
  private static final String TABLE_NAME = "table";
  private HashMapDB db = new HashMapDB();

  @Before
  public void setUp() throws Exception {
    db.init();
  }

  @After
  public void tearDown() throws Exception {
    db.cleanup();
  }

  @Test
  public void testRead() throws Exception {
    final String rowKey = "row1";
    ConcurrentNavigableMap<String, Map<String, byte[]>> table = db.getTable(TABLE_NAME);
    Map<String, byte[]> row = new HashMap<>();
    row.put("column1", new StringByteIterator("value1").toArray());
    row.put("column2", new StringByteIterator("value2").toArray());
    table.put(rowKey, row);

    final HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    final Status status = db.read(TABLE_NAME, rowKey, null, result);
    assertEquals(Status.OK, status);
    assertEquals(2, result.size());
    assertEquals("value1", result.get("column1").toString());
    assertEquals("value2", result.get("column2").toString());
  }

  @Test
  public void testInsert() throws Exception {
    final String rowKey = "row1";
    final HashMap<String, ByteIterator> row = new HashMap<>();
    row.put("n1", new StringByteIterator("v1"));
    row.put("n2", new StringByteIterator("v2"));
    final Status status = db.insert(TABLE_NAME, rowKey, row);

    assertEquals(Status.OK, status);
    assertEquals(1, db.getTable(TABLE_NAME).size());
    assertEquals("v1", new String(db.getTable(TABLE_NAME).get(rowKey).get("n1")));
    assertEquals("v2", new String(db.getTable(TABLE_NAME).get(rowKey).get("n2")));
  }
}
