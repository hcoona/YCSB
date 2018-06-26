package com.yahoo.ycsb.db.kv;

import java.io.File;
import java.util.Properties;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * The BerkeleyDB implementation of a simple key-value store.
 */
public class BerkeleyDBKVStoreEngine implements ISimpleKVStoreEngine {
  private Environment env;
  private Database db;

  @Override
  public void init(Properties properties) throws Exception {
    final EnvironmentConfig conf = new EnvironmentConfig();
    conf.setAllowCreate(true);

    final String envHomePath = properties.getProperty("simplekv.berkeleydb.env-home.path");
    final File envHomeFile = new File(envHomePath);
    if (envHomeFile.exists()) {
      envHomeFile.delete();
    }
    envHomeFile.mkdirs();
    env = new Environment(
        envHomeFile,
        conf
    );

    final DatabaseConfig dbConf = new DatabaseConfig();
    dbConf.setAllowCreate(true);
    dbConf.setTemporary(true);

    db = env.openDatabase(
        null,
        "db",
        dbConf
    );
    //env.truncateDatabase(null, db.getDatabaseName(), false);
  }

  @Override
  public void cleanup() {
    db.close();
    env.close();

    db = null;
    env = null;
  }

  @Override
  public byte[] get(String key) throws Exception {
    DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry dbValue = new DatabaseEntry();
    OperationStatus dbStatus = db.get(null, dbKey, dbValue, LockMode.DEFAULT);
    if (dbStatus == OperationStatus.SUCCESS) {
      return dbValue.getData();
    } else {
      throw new Exception("OperationStatus = " + dbStatus);
    }
  }

  @Override
  public void put(String key, byte[] value) throws Exception {
    DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
    DatabaseEntry dbValue = new DatabaseEntry(value);
    OperationStatus dbStatus = db.put(null, dbKey, dbValue);
    if (dbStatus != OperationStatus.SUCCESS) {
      throw new Exception("OperationStatus = " + dbStatus);
    }
  }

  @Override
  public void delete(String key) throws Exception {
    DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
    OperationStatus dbStatus = db.delete(null, dbKey);
    if (dbStatus != OperationStatus.SUCCESS) {
      throw new Exception("OperationStatus = " + dbStatus);
    }
  }
}
