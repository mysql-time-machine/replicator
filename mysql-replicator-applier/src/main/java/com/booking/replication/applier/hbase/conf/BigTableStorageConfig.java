package com.booking.replication.applier.hbase.conf;

import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.applier.hbase.StorageConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;

public class BigTableStorageConfig implements StorageConfig {

    private final Map<String, Object> configuration;

    public BigTableStorageConfig(Map<String, Object> configuration) {
        this.configuration = configuration;
    }

    @Override
    public org.apache.hadoop.conf.Configuration getConfig() {

        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        // Client impl
        config.set("hbase.client.connection.impl", "com.google.cloud.bigtable.hbase1_x.BigtableConnection");

        // These two are not necessary if project.id and instance.id are in hbase-site.xml
        // The reason why we prefer to have them as params is so that we don't need separate
        // build in order to change the target BigTable instance
        String projectId = (String) configuration.get(StorageConfig.Configuration.BIGTABLE_PROJECT_ID);
        String instanceId = (String) configuration.get(StorageConfig.Configuration.BIGTABLE_INSTANCE_ID);
        config.set("google.bigtable.project.id", projectId);
        config.set("google.bigtable.instance.id", instanceId);

        return config;
    }
}
