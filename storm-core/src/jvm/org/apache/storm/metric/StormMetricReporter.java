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

import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;

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

    public StormMetricReporter(MetricRegistry registry, LocalState state, String stormId, int port,
                               String name, MetricFilter filter, 
                               TimeUnit rateUnit, TimeUnit durationUnit) throws org.apache.thrift.transport.TTransportException {
        super(registry, name, filter, rateUnit, durationUnit);

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
                workerStats.set_time_stamp(_reportTime);
                workerStats.put_to_metrics(key, new Double(count));

                try {
                    client.consumeMetric(key, count);
                } catch (TException ex){
                    LOG.error("TException consuming: {}", ex);
                } catch (Exception ex){
                    LOG.error("Exception consuming: {}", ex);
                }
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

                try {
                    client.consumeMetric(key, count);
                } catch (TException ex){
                    LOG.error("TException consuming: {}", ex);
                } catch (Exception ex){
                    LOG.error("Exception consuming: {}", ex);
                }
            }
        }
        state.setWorkerStats(workerStats);
    }
}
