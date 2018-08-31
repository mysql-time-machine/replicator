package com.booking.replication.applier;

import com.booking.replication.applier.kafka.KafkaSeeker;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public interface Seeker extends Function<Collection<AugmentedEvent>, Collection<AugmentedEvent>>, Closeable {
    enum Type {
        NONE {
            @Override
            public Seeker newInstance(Map<String, Object> configuration) {
                return eventList -> eventList;
            }
        },
        KAFKA {
            @Override
            public Seeker newInstance(Map<String, Object> configuration) {
                return new KafkaSeeker(configuration);
            }
        };

        public abstract Seeker newInstance(Map<String, Object> configuration);
    }

    interface Configuration {
        String TYPE = "seeker.type";
    }

    default Checkpoint seek(Checkpoint checkpoint) {
        return checkpoint;
    }

    @Override
    default void close() throws IOException {
    }

    static Seeker build(Map<String, Object> configuration) {
        return Seeker.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name()).toString()
        ).newInstance(configuration);
    }
}
