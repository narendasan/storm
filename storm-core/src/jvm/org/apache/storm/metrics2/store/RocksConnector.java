/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.storm.metrics2.store;

import java.lang.String;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;


public class RocksConnector {

    private RocksDB db;
    public RocksConnector(String dbfilename)
    {
        RocksDB.loadLibrary();
        // the Options class contains a set of configurable DB options
        // that determines the behavior of a database.
        Options options = new Options().setCreateIfMissing(true);
        this.db = null;
        try {
            // a factory method that returns a RocksDB instance
            this.db = RocksDB.open(options, dbfilename);
            // do something
        } catch (RocksDBException e) {
            System.out.println(e);
        }
    }

    public void insert(Metric m)
    {
        try
        {
            this.db.put(m.serialize().getBytes(), m.getValue().getBytes());
        }
        catch(RocksDBException e)
        {
            System.out.println("oh no!");
        }
    }

    public void remove()
    {

    }

    public List<String> scan()
    {
        List<String> result = new ArrayList<String>();
        RocksIterator iterator = this.db.newIterator();
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            result.add(String.format("%s", new String(iterator.key())));
        }
        return result;
    }

    public List<String> scan(String prefix)
    {
        List<String> result = new ArrayList<String>();
        RocksIterator iterator = this.db.newIterator();
        for (iterator.seek(prefix.getBytes()); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            if (key.startsWith(prefix)) {
                result.add(String.format("%s", new String(iterator.key())));
            }
        }
        return result;
    }

    public List<String> scan(HashMap settings)
    {
        List<String> result = new ArrayList<String>();
        RocksIterator iterator = this.db.newIterator();
        for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
            String key = new String(iterator.key());
            String[] elements = key.split("\\|");

            if (!(settings.containsKey("metric") && !elements[0].equals(settings.get("metric")) ||
                    settings.containsKey("compId") && !elements[3].equals(settings.get("compId")) ||
                    settings.containsKey("topoId") && !elements[4].equals(settings.get("topoId")))){
                result.add(String.format("%s", new String(iterator.value())));
            }
        }
        return result;
    }

}
