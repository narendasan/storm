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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;

import org.apache.storm.generated.Nimbus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormMetricReporter extends ScheduledReporter {
    TTransport transport;
    Nimbus.Client client;

    private static final Logger LOG = LoggerFactory.getLogger(StormMetricReporter.class);

    public StormMetricReporter(MetricRegistry registry, String name, MetricFilter filter, 
                               TimeUnit rateUnit, TimeUnit durationUnit) throws org.apache.thrift.transport.TTransportException {
        super(registry, name, filter, rateUnit, durationUnit);

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

        LOG.info("Got call to report with gauges: {} counters: {} histograms: {} meters: {} timers: {}",
                gauges == null   ? "null" : gauges.size(),
                counters == null ? "null" : counters.size(),
                histograms == null ? "null" : histograms.size(),
                meters == null ? "null" : meters.size(),
                timers == null ? "null" : timers.size());
        
        if (counters != null) {
            for (Map.Entry<String, Counter> c : counters.entrySet()) {
                try {
                    client.consumeMetric(c.getKey(), c.getValue().getCount());
                } catch (TException ex){
                    LOG.error("TException consuming: {}", ex);
                } catch (Exception ex){
                    LOG.error("Exception consuming: {}", ex);
                }
            }
        }
    }
}
