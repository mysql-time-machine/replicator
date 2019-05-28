package com.booking.replication.applier;

import com.booking.replication.applier.console.ConsoleApplier;
import com.booking.replication.applier.count.CountApplier;
import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public interface Applier extends Function<Collection<AugmentedEvent>, Boolean>, Closeable {

    Logger LOG = LogManager.getLogger(Applier.class);

    boolean forceFlush();

    enum Type {
        CONSOLE {
            @Override
            protected Applier newInstance(Map<String, Object> configuration) {
                return new ConsoleApplier(configuration);
            }
        },
        HBASE {
            @Override
            protected Applier newInstance(Map<String, Object> configuration)  {
                return new HBaseApplier(configuration);
            }
        },
        KAFKA {
            @Override
            protected Applier newInstance(Map<String, Object> configuration) {
                return new KafkaApplier(configuration);
            }
        },
        COUNT {
            @Override
            protected Applier newInstance(Map<String, Object> configuration) {
                return new CountApplier(configuration);
            }
        };

        protected abstract Applier newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "applier.type";
    }

    @Override
    default void close() throws IOException {
    }

    static Applier build(Map<String, Object> configuration) {
        try {
            return Type.valueOf(
                    configuration.getOrDefault(Configuration.TYPE, Type.HBASE.name()).toString()
            ).newInstance(configuration);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }
}
