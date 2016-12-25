package org.apache.storm.metrics2.store;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

public class MetricStoreConfigTest {

    @Test
    public void testConfigureSuccess() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksDBConnector");
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "true");
        MetricStoreConfig storeConf = new MetricStoreConfig();
        MetricStore store = storeConf.configure(config);
    }

    @Test(expected=MetricException.class)
    public void testConfigureNoClass() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        MetricStoreConfig storeConf = new MetricStoreConfig();
        MetricStore store = storeConf.configure(config);
    }


    @Test(expected=MetricException.class)
    public void testConfigureWrongClass() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.Rocksonnector");
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        MetricStoreConfig storeConf = new MetricStoreConfig();
        MetricStore store = storeConf.configure(config);
    }
}