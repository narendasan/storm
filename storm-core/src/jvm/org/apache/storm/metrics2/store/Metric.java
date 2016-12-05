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
import java.lang.StringBuilder;

public class Metric {

    private String metricName;
    private String topoId;
    private String host;
    private int port;
    private String componentName;
    private String execId;
    private String compId;
    private long timestamp;
    private String value;
    private String dimensions;

    public String getValue()
    {
        return value;
    }

    public Metric(String metric, long TS, String execId, String compId, String topoId, String value)
    {
        this.metricName = metric;
        this.timestamp = TS;
        this.execId = execId;
        this.compId = compId;
        this.topoId = topoId;
        this.value = value;
    }

    public String serialize()
    {
        StringBuilder x = new StringBuilder();
        x.append(this.metricName);
        x.append('|');
        x.append(this.timestamp);
        x.append('|');
        x.append(this.execId);
        x.append('|');
        x.append(this.compId);
        x.append('|');
        x.append(this.topoId);

        return String.valueOf(x);
    }
}
