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

import java.util.List;
import java.util.HashMap;

public class Aggregation {

    // Rocks component
    private RocksConnector connector;

    // Key components
    private HashMap settings = new HashMap();

    public Aggregation(RocksConnector connector) {
        this.connector = connector;
    }

    // Filter for specific fields
    // Todo: Filter for different instances of the same field, two hosts for example

    public void filterMetric(String metric) {
        this.settings.put("metric", metric);
    }

    public void filterTopo(String topoId) {
        this.settings.put("topoId", topoId);
    }

    public void filterHost(String host) {
        this.settings.put("host", host);
    }

    public void filterPort(String port) {
        this.settings.put("port", port);
    }

    public void filterComp(String comp) {
        this.settings.put("compId", comp);
    }

    // Aggregations

    public Double sum() throws MetricException {
        Double sum = 0.0;
        List<String> x = this.connector.scan(settings);
        for(String each : x) {
            sum += Double.parseDouble(each);
        }
        return sum;
    }

    public Double min() throws MetricException {
        Double min = Double.MAX_VALUE;
        List<String> x = this.connector.scan(settings);
        for(String each : x) {
            Double curr = Double.parseDouble(each);
            if(curr < min) {
                min = curr;
            }
        }
        return min;
    }

    public Double max() throws MetricException {
        Double max = Double.MIN_VALUE;
        List<String> x = this.connector.scan(settings);
        for(String each : x) {
            Double curr = Double.parseDouble(each);
            if(curr > max) {
                max = curr;
            }
        }
        return max;
    }

    public Double mean() throws MetricException {
        Double sum = 0.0;
        Integer count = 0;
        List<String> x = this.connector.scan(settings);
        for(String each : x) {
            sum += Integer.parseInt(each);
            count++;
        }
        return sum / count;
    }

}
