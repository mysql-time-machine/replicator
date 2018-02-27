package com.booking.replication.applier;

import com.booking.replication.applier.kafka.KafkaEventSeeker;
import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;

import java.util.Map;
import java.util.function.Function;

public interface EventSeeker extends Function<Event, Event> {
    enum Type {
        NONE {
            @Override
            public EventSeeker newInstance(Map<String, String> configuration) {
                return event -> event;
            }
        },
        KAFKA {
            @Override
            public EventSeeker newInstance(Map<String, String> configuration) {
                return new KafkaEventSeeker(configuration);
            }
        };

        public abstract EventSeeker newInstance(Map<String, String> configuration);
    }

    interface Configuration {
        String TYPE = "seeker.type";
    }

    static EventSeeker build(Map<String, String> configuration) {
        return EventSeeker.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration);
    }
}
