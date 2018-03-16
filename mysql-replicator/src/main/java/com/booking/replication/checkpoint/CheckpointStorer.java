package com.booking.replication.checkpoint;

import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;

import java.util.Map;
import java.util.function.Consumer;

public interface CheckpointStorer extends Consumer<Event> {
    enum Type {
        NONE {
            @Override
            public <Destination> CheckpointStorer newInstance(Map<String, String> configuration, Destination destination) {
                return event -> {};
            }
        },
        COORDINATOR {
            @Override
            public <Destination> CheckpointStorer newInstance(Map<String, String> configuration, Destination destination) {
                Coordinator coordinator = Coordinator.class.cast(destination);

                return new CoordinatorCheckpointStorer(
                        coordinator,
                        configuration.getOrDefault(
                                CheckpointStorer.Configuration.PATH,
                                coordinator.defaultCheckpointPath()
                        )
                );
            }
        };

        public abstract <Destination> CheckpointStorer newInstance(Map<String, String> configuration, Destination destination);
    }

    interface Configuration {
        String TYPE = "checkpoint.storer.type";
        String PATH = "checkpoint.storer.path";
    }

    static <Destination> CheckpointStorer build(Map<String, String> configuration, Destination destination) {
        return CheckpointStorer.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration, destination);
    }
}
