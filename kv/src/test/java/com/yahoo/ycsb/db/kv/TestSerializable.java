package com.yahoo.ycsb.db.kv;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class TestSerializable {
  @Test
  public void testSerializeImmutableMap() throws IOException, ClassNotFoundException {
    ImmutableMap<String, String> data = ImmutableMap.of("n1", "v1", "n2", "v2");
    byte[] dataBytes = SerializationUtils.serialize(data);
    ImmutableMap<String, String> data2 = SerializationUtils.deserialize(dataBytes);

    System.out.println(data2.getClass().getCanonicalName());
    System.out.println(data2);
    Assert.assertEquals(2, data2.size());
  }
}
