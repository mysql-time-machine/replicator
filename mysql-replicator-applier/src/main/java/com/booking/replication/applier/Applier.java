package com.booking.replication.applier;

import com.booking.replication.applier.console.ConsoleApplier;
import com.booking.replication.applier.hbase.HBaseApplier;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.event.AugmentedEvent;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public interface Applier extends Function<Collection<AugmentedEvent>, Boolean>, Closeable {
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
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.HBASE.name()).toString()
        ).newInstance(configuration);
    }
}
