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

import org.apache.storm.store;

//TODO: LOOK AT JStormMetricsCache


/**
 *    This class looks to implement a first version of a metric store
 **/
public class TimeseriesMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(TimeseriesMetrics.class);

    protected MetricStore TimeseriesMetricsStore = null;

    public TimeseriesMetrics() {
        /* TODO; CREATE A RocksDBMetricStore then store in MetricStore */
        LOG.info("Creating a RocksDB store for TimeseriesMetrics");

        try {
            TimeseriesMetricsStore.init(/* Fill in conf parameters*/);
        } catch {
            LOG.error("Failed to create a RocksDB Store");
        }
    }

    public MetricStore getStore() {
        return TimeseriesMetricsStore;
    }

    //ASK: ARE THESE FUNCTIONS NECESSARY?
    public Object get(String k) {
        return TimeseriesMetricsStore.get(k);
    }

    public void put(String k, Object v) {
        TimeseriesMetricsStore.put(k,v);
        return;
    }

    public void remove(String k) {
        TimeseriesMetricsStore.remove(k);
        return;
    }
}
