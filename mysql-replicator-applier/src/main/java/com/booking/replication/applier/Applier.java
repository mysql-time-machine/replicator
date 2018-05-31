package com.booking.replication.applier;

import com.booking.replication.applier.console.ConsoleApplier;
import com.booking.replication.applier.kafka.KafkaApplier;
import com.booking.replication.augmenter.model.AugmentedEvent;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Consumer;

public interface Applier extends Consumer<AugmentedEvent>, Closeable {
    enum Type {
        CONSOLE {
            @Override
            public Applier newInstance(Map<String, String> configuration) {
                return new ConsoleApplier(configuration);
            }
        },
        KAFKA {
            @Override
            public Applier newInstance(Map<String, String> configuration) {
                return new KafkaApplier(configuration);
            }
        };

        public abstract Applier newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "applier.type";
    }

    static Applier build(Map<String, String> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.CONSOLE.name())
        ).newInstance(configuration);
    }
}
