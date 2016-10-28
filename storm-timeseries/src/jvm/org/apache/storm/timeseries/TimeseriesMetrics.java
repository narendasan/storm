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
package org.apache.storm.timeseries;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;

/**
 *    This class looks to implement a first version of a metric store
 **/
public class TimeseriesMetrics {
    /**
     * Creates the RocksDB instance to store metrics from Nimbus
     * @param list of timestamps
     **/
    public void initDB(List<Integer> list) throws Exception {
        //TODO: CHANGE THIS TO THE STORM LOGGER IF THERE IS ONE
        System.out.Println("Intializing Metrics DB");

        DBOptions dbOptions = null;
        List<ColumnFamilyDescriptior> columnFamilyNames = new ArrayList<ColumnFamilyDescriptior>();
        columnFamilyNames.add(new ColumnFamilyDescriptior(RocksDB.DEFAULT_COLUMN_FAMILY));
        for (Integer timeout : list) {
            columnFamilyNames.add(new ColumnFamilyDescriptor(String.valueOf(timeout).getBytes()));
        }
    }

}
