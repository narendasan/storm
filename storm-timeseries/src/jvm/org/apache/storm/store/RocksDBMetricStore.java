/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.store;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

// LOTS OF THIS IS TAKEN FROM JSTORM
public class RocksDBMetricStore implements MetricStore {

    static {
        RocksDB.loadLibrary();
    }

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMetricStore.class);
    public static final String ROCKSDB_ROOT_DIR = "rocksdb.root.dir";
    public static final String ROCKSDB_RESET = "rocksdb.reset";
    private static final String MAX_CREATE_RETRIES = 3;
    protected RocksDB store;
    protected String rootDir;

    @Override
    public void init(Map<Object, Object> config) throws Exception {
        this,initDir(config);

        List<Integer> list = new ArrayList<Integer>();

        if (config.get(TAG_TIMEOUT_LIST) != null) {
            for (Object o : (List) ConfigExtension.getCacheTimeoutList(config)) { // ASK WHAT IS CONFIG EXTENTION DO?
                Integer timeout = (Integer) o;
                if (timeout == null || timeout <= 0) {
                    continue;
                }
                list.add(timeout);
            }
        }

        boolean succeeded = false;
        for (int i = 0; i < MAX_CREATE_RETRIES; i++) { //
            try {
                initDb(list);
                succeeded = true;
            } catch (Exception e) {
                LOG.warn("Failed to initize RocksDB in " + rootDir + "(Try " + i + " of " + MAX_CREATE_RETRIES + ")", e);
                try {
                     PathUtils.rmr(rootDir);
                } catch (IOException ignore) {}
            }
        }

        if (succeeded != true) {
            throw new RuntimeException("Failed to intialize a RocksDB in " + rootDir);
        }
    }

    @Override
    public void teardown() {
        LOG.info("Begin teardown of RocksDB in " + rootDir); //ASK: Is there a storm logger?
        if (store != null) {
            store.close();
        }
        LOG.info("Finished teardown of RocksDB in " + rootDir); //ASK: Is there a storm logger?
    }

    @Override
    public Object get(String key) {
        try {
            byte[] data = db.get(key.getBytes());
            if (data != null) {
                try {
                    return Utils.javaDeserialize(data); //TODO: DROP SERIALIZATION
                } catch (Exception e) {
                    LOG.error("Failed to retrive value of " + key);
                    store.remove(key.getBytes());
                    return null;
                }
            }
        } catch (Exception ignore) {}

        return null;
    }

    @Override
    public void put(String key, Object value) {
        byte[] data = Utils.javaSerialize(value); //TODO DROP SERIALIZATION
        try {
            store.put(key.getBytes(), data);
        } catch (Exception e) {
            LOG.error("Failed to store " + key);
        }
    }

    @Override
    public void remove(String key) {
        try {
            store.remove(key.getBytes());
        } catch (Exception e) {
            LOG.error("Failed to remove " + key);
        }
    }

    /* Setup the root directory for RocksDB*/
    private void initDir(Map<Object, Object> config) {
        String configDir = (String) config.get(ROCKSDB_ROOT_DIR);
        if (StringUtils.isBlank(configDir) == true) {
            throw new RuntimeException("Failed get a valid root directory for RocksDB");
        }

        boolean clean = (boolean) config.get(ROCKSDB_RESET);
        LOG.info("RocksDB reset is " + clean);
        if (clean == true) {
            try {
                PathUtils.rmr(configDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to clean the specified root directory for RocksDB");
            }
        }

        File file = new File(configDir);
        if (file.exists() == false) {
            try {
                PathUtils.local_mkdirs(configDir);
                file = new File(configDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create the specified root directory for RocksDB");
            }
        }

        rootDir = file.getAbsolutePath();
    }

    private void initDB(List<Integer> list) throws Exception {
        LOG.info("Initializing RocksDB in " + rootDir);

        Options rocksDBOptions = null;
        try {
            rocksDBOptions = new Options().setCreateMissingColumnFamilies(true).setCreateIfMissing(true);
            List<ColumnFamilyHandler> ColumnFamilyHandleList = new ArrayList<ColumnFamilyHandle>();
            store = RocksDB.open(rocksDBOptions, rootDir);
            LOG.info("Successful Initialization of RocksDB in " + rootDir);
        } finally {
            if (rocksDBOptions != null) {
                rocksDBOptions.dispose();
            }
        }
    }



}
