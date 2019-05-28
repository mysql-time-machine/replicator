package com.booking.replication.applier.hbase;

import com.booking.replication.applier.hbase.conf.BigTableStorageConfig;
import com.booking.replication.applier.hbase.conf.HBaseStorageConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public interface StorageConfig {

    Logger LOG = LogManager.getLogger(StorageConfig.class);

    org.apache.hadoop.conf.Configuration getConfig();

    enum Type {
        HBASE {
            @Override
            protected StorageConfig newInstance(Map<String, Object> configuration) {
                return new HBaseStorageConfig(configuration);
            }
        },
        BIGTABLE {
            @Override
            protected StorageConfig newInstance(Map<String, Object> configuration)  {
                return new BigTableStorageConfig(configuration);
            }
        };

        protected abstract StorageConfig newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE                  = "applier.hbase.storage.type";
        String BIGTABLE_PROJECT_ID   = "applier.bigtable.project.id";
        String BIGTABLE_INSTANCE_ID  = "applier.bigtable.instance.id";
    }

    static StorageConfig build(Map<String, Object> configuration) {
        try {
            return StorageConfig.Type.valueOf(
                    configuration.getOrDefault(
                            StorageConfig.Configuration.TYPE,
                            StorageConfig.Type.HBASE.name()
                    ).toString()
            ).newInstance(configuration);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }
}
