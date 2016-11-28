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
package org.apache.storm.metric;

import java.util.concurrent.TimeUnit;
import java.util.SortedMap;
import java.util.Map;
import java.util.HashMap;

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.MetricRegistryListener;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;

import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.LSWorkerStats;

import org.apache.storm.utils.LocalState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMetricReporter extends ScheduledReporter {
    TTransport transport;
    Nimbus.Client client;
    LocalState state;
    String stormId;
    int port;
    long _reportTime = 0;
    long _prevReportTime = 0;

    private static final Logger LOG = LoggerFactory.getLogger(StormMetricReporter.class);

    private Map<String, Long> counterCache;

    public StormMetricReporter(MetricRegistry registry, LocalState state, String stormId, int port,
                               String name, MetricFilter filter, 
                               TimeUnit rateUnit, TimeUnit durationUnit) throws org.apache.thrift.transport.TTransportException {
        super(registry, name, filter, rateUnit, durationUnit);

        counterCache = new HashMap<String, Long>();

        // compute cache
        registry.addListener(new MetricRegistryListener() {
            @Override
            public void onCounterRemoved(String name){
                counterCache.remove(name);
            }

            @Override
            public void onCounterAdded(String name, Counter counter){
                counterCache.put(name, new Long(0)); // always start this at 0, the report function updates
            }

            @Override
            public void onGaugeAdded(String name, Gauge<?> gauge) {}

            @Override
            public void onGaugeRemoved(String name) {}

            @Override
            public void onHistogramAdded(String name, Histogram hist) {}

            @Override
            public void onHistogramRemoved(String name) {}

            @Override
            public void onMeterAdded(String name, Meter meter) {}

            @Override
            public void onMeterRemoved(String name) {}

            @Override
            public void onTimerAdded(String name, Timer timer) {}

            @Override
            public void onTimerRemoved(String name) {}
        });

        this.state = state;
        this.stormId = stormId;
        this.port = port;

        //TODO get from config

        transport = new TSocket("localhost", 6627);
        TFramedTransport ft = new TFramedTransport(transport);
        ft.open();

        TProtocol protocol = new TBinaryProtocol(ft);
        client = new Nimbus.Client(protocol);
    }

    @Override
    public void report(SortedMap<String,Gauge> gauges, 
                SortedMap<String,Counter> counters, 
                SortedMap<String,Histogram> histograms, 
                SortedMap<String,Meter> meters, 
                SortedMap<String,Timer> timers){

        _prevReportTime = _reportTime;
        _reportTime = System.currentTimeMillis();

        LOG.info("Got call to report at {} ({} previous) -- with gauges: {} counters: {} histograms: {} meters: {} timers: {}",
                _reportTime, _prevReportTime,
                gauges == null   ? "null" : gauges.size(),
                counters == null ? "null" : counters.size(),
                histograms == null ? "null" : histograms.size(),
                meters == null ? "null" : meters.size(),
                timers == null ? "null" : timers.size());
       
        LSWorkerStats workerStats = new LSWorkerStats(); 
        if (counters != null) {
            for (Map.Entry<String, Counter> c : counters.entrySet()) {
                String key = c.getKey();
                long count = c.getValue().getCount();
                // check the cache to see if the value changed
                long oldCount = counterCache.get(key);

                // report the delta between the old count and the new count
                // if count is positive, take diff
                // oldCount is always <= count (counters are always increasing)
                long reportCount = count > 0 ? count - oldCount : 0;

                // for the next call
                counterCache.put(key, count);

                workerStats.set_time_stamp(_reportTime);
                workerStats.put_to_metrics(key, new Double(reportCount));
            }
        }
        if (histograms != null) {
            for (Map.Entry<String, Histogram> c : histograms.entrySet()) {
                String key = c.getKey();
                Histogram t = c.getValue();
                double count = t.getCount();
                Snapshot snap = t.getSnapshot();
                double pct75  = snap.get75thPercentile();
                double pct95  = snap.get95thPercentile();
                double pct98  = snap.get98thPercentile();
                double pct99  = snap.get99thPercentile();
                double pct999 = snap.get999thPercentile();
                long max      = snap.getMax();
                long min      = snap.getMin();
                double means  = snap.getMean();
                double median = snap.getMedian();
                double stddev = snap.getStdDev();
                long[] vals   = snap.getValues();

                //workerStats.put_to_metrics(key + "--pct75" , new Double(pct75));
                //workerStats.put_to_metrics(key + "--pct95" , new Double(pct95));
                //workerStats.put_to_metrics(key + "--pct98" , new Double(pct98));
                //workerStats.put_to_metrics(key + "--pct99" , new Double(pct99));
                //workerStats.put_to_metrics(key + "--pct999", new Double(pct999));
                workerStats.put_to_metrics(key + "--max"   , new Double(max));
                workerStats.put_to_metrics(key + "--min"   , new Double(min));
                workerStats.put_to_metrics(key + "--means" , new Double(means));
               // workerStats.put_to_metrics(key + "--median", new Double(median));
               // workerStats.put_to_metrics(key + "--stddev", new Double(stddev));
               //for (int i = 0; i < vals.length; i++){
               //    workerStats.put_to_metrics(key + "--vals" + i,   new Double(vals[i]));
               //}
            }
        }
        if (timers != null) {
            for (Map.Entry<String, Timer> c : timers.entrySet()) {
                String key = c.getKey();
                Timer t = c.getValue();
                long count = t.getCount();
                double rate15 = t.getFifteenMinuteRate();
                double rate5  = t.getFiveMinuteRate();
                double rate1  = t.getOneMinuteRate();
                double mean   = t.getMeanRate();
                Snapshot snap = t.getSnapshot();
                double pct75  = snap.get75thPercentile();
                double pct95  = snap.get95thPercentile();
                double pct98  = snap.get98thPercentile();
                double pct99  = snap.get99thPercentile();
                double pct999 = snap.get999thPercentile();
                long max      = snap.getMax();
                long min      = snap.getMin();
                double means  = snap.getMean();
                double median = snap.getMedian();
                double stddev = snap.getStdDev();
                long[] vals   = snap.getValues();

                workerStats.put_to_metrics(key + "--count" , new Double(count));
                workerStats.put_to_metrics(key + "--rate15", new Double(rate15));
                workerStats.put_to_metrics(key + "--rate5" , new Double(rate5));
                workerStats.put_to_metrics(key + "--rate1" , new Double(rate1));
                workerStats.put_to_metrics(key + "--mean"  , new Double(mean));
                workerStats.put_to_metrics(key + "--pct75" , new Double(pct75));
                workerStats.put_to_metrics(key + "--pct95" , new Double(pct95));
                workerStats.put_to_metrics(key + "--pct98" , new Double(pct98));
                workerStats.put_to_metrics(key + "--pct99" , new Double(pct99));
                workerStats.put_to_metrics(key + "--pct999", new Double(pct999));
                workerStats.put_to_metrics(key + "--max"   , new Double(max));
                workerStats.put_to_metrics(key + "--min"   , new Double(min));
                workerStats.put_to_metrics(key + "--means" , new Double(means));
                workerStats.put_to_metrics(key + "--median", new Double(median));
                workerStats.put_to_metrics(key + "--stddev", new Double(stddev));
                for (int i = 0; i < vals.length; i++){
                    workerStats.put_to_metrics(key + "--vals" + i,   new Double(vals[i]));
                }
            }
        }
        state.setWorkerStats(workerStats);
    }
}
