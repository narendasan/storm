package org.apache.storm.metrics2.store;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static org.junit.Assert.*;

public class RocksDBConnectorTest {

    @Test
    public void testPrepareSuccess() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksDBConnector");
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "true");
        RocksDBConnector store = new RocksDBConnector();
        store.prepare(config);
    }

    @Test
    public void testPrepareSuccessOnExistingDB() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksDBConnector");
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        RocksDBConnector store = new RocksDBConnector();
        store.prepare(config);
    }

    @Test
    public void testPrepareValidateonCreateIfMissing() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksDBConnector");
        config.put("storm.metrics2.store.rocksdb.location", "other_db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        RocksDBConnector store = new RocksDBConnector();
        store.prepare(config);
    }

    @Test(expected=MetricException.class)
    public void testPrepareValidateFailMisnamed() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksConnector");
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        RocksDBConnector store = new RocksDBConnector();
        store.prepare(config);
    }

    @Test(expected=MetricException.class)
    public void testPrepareValidateFailMissingField() {
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksDBConnector");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        RocksDBConnector store = new RocksDBConnector();
        store.prepare(config);
    }

    @Test
    public void testInsert() throws Exception {
        Metric test = new Metric("__sendqueue:population", Long.parseLong("1478209815"), "2", "my-test-topology", "0");
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksDBConnector");
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        RocksDBConnector store = new RocksDBConnector();
        store.prepare(config);
        store.insert(test);
    }

    @Test
    public void testScanPrefix() throws Exception {

    }

    @Test
    public void testScan() throws Exception {
        Metric test = new Metric("__sendqueue:population", Long.parseLong("1478209815"), "2", "my-test-topology", "0");
        Map config = new HashMap();
        config.put("storm.metrics2.store.connector_class", "org.apache.storm.metrics2.store.RocksDBConnector");
        config.put("storm.metrics2.store.rocksdb.location", "db.test");
        config.put("storm.metrics2.store.rocksdb.create_if_missing", "false");
        RocksDBConnector store = new RocksDBConnector();
        store.prepare(config);
        store.insert(test);
        List<String> l = store.scan();
        String[] arr = l.toArray(new String[l.size()]);
        String[] expected = ["my-test-topology", "__sendqueue:population", "1478209815", "2", "null", "null", "0", "null"];
        Assert.assertArrayEquals(expected, arr);
    }

}