package com.booking.replication.applier;

import com.booking.replication.applier.kafka.KafkaEventSeeker;
import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;

import java.util.Map;
import java.util.function.Function;

public interface EventSeeker extends Function<AugmentedEvent, AugmentedEvent> {
    enum Type {
        NONE {
            @Override
            public EventSeeker newInstance(Map<String, String> configuration, Checkpoint checkpoint) {
                return event -> event;
            }
        },
        KAFKA {
            @Override
            public EventSeeker newInstance(Map<String, String> configuration, Checkpoint checkpoint) {
                return new KafkaEventSeeker(configuration, checkpoint);
            }
        };

        public abstract EventSeeker newInstance(Map<String, String> configuration, Checkpoint checkpoint);
    }

    interface Configuration {
        String TYPE = "seeker.type";
    }

    static EventSeeker build(Map<String, String> configuration, Checkpoint checkpoint) {
        return EventSeeker.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration, checkpoint);
    }
}
