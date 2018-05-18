package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.coordinator.Coordinator;
import com.booking.replication.supplier.model.RawEvent;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public interface CheckpointStorer extends BiConsumer<AugmentedEvent, Map<AugmentedEvent, AtomicReference<AugmentedEvent>>> {
    enum Type {
        NONE {
            @Override
            public <Destination> CheckpointStorer newInstance(Map<String, String> configuration, Destination destination) {
                return (event, map) -> {
                };
            }
        },
        COORDINATOR {
            @Override
            public <Destination> CheckpointStorer newInstance(Map<String, String> configuration, Destination destination) {
                Coordinator coordinator = Coordinator.class.cast(destination);

                return new CoordinatorCheckpointStorer(
                        coordinator,
                        configuration.get(CheckpointStorer.Configuration.PATH)
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
