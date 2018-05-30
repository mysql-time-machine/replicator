package com.booking.replication.applier;

import com.booking.replication.applier.console.ConsoleEventApplier;
import com.booking.replication.applier.kafka.KafkaEventApplier;
import com.booking.replication.augmenter.model.AugmentedEvent;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Consumer;

public interface EventApplier extends Consumer<AugmentedEvent>, Closeable {
    enum Type {
        CONSOLE {
            @Override
            public EventApplier newInstance(Map<String, String> configuration) {
                return new ConsoleEventApplier(configuration);
            }
        },
        KAFKA {
            @Override
            public EventApplier newInstance(Map<String, String> configuration) {
                return new KafkaEventApplier(configuration);
            }
        };

        public abstract EventApplier newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "applier.type";
    }

    static EventApplier build(Map<String, String> configuration) {
        return Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.CONSOLE.name())
        ).newInstance(configuration);
    }
}
