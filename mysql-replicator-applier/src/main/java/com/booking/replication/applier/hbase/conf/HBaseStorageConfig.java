package com.booking.replication.applier.hbase.conf;

import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.applier.hbase.StorageConfig;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;

public class HBaseStorageConfig implements StorageConfig {

    private final Map<String, Object> configuration;

    public HBaseStorageConfig(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfig() {

        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        String ZOOKEEPER_QUORUM =
                (String) configuration.get(HBaseApplier.Configuration.HBASE_ZOOKEEPER_QUORUM);

        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);

        return conf;
    }
}
