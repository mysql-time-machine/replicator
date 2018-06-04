package com.booking.replication.applier;

import com.booking.replication.applier.kafka.KafkaSeeker;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;

import java.util.Map;
import java.util.function.Function;

public interface Seeker extends Function<AugmentedEvent, AugmentedEvent> {
    enum Type {
        NONE {
            @Override
            public Seeker newInstance(Map<String, String> configuration, Checkpoint checkpoint) {
                return event -> event;
            }
        },
        KAFKA {
            @Override
            public Seeker newInstance(Map<String, String> configuration, Checkpoint checkpoint) {
                return new KafkaSeeker(configuration, checkpoint);
            }
        };

        public abstract Seeker newInstance(Map<String, String> configuration, Checkpoint checkpoint);
    }

    interface Configuration {
        String TYPE = "seeker.type";
    }

    default void seek(Checkpoint checkpoint) {}

    static Seeker build(Map<String, String> configuration, Checkpoint checkpoint) {
        return Seeker.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration, checkpoint);
    }
}
