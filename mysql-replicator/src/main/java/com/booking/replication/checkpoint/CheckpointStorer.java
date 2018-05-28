package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.supplier.model.checkpoint.CheckpointStorage;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public interface CheckpointStorer extends BiConsumer<AugmentedEvent, Map<AugmentedEvent, AtomicReference<AugmentedEvent>>> {
    enum Type {
        NONE {
            @Override
            public CheckpointStorer newInstance(Map<String, String> configuration, CheckpointStorage checkpointStorage) {
                return (event, map) -> {
                };
            }
        },
        COORDINATOR {
            @Override
            public CheckpointStorer newInstance(Map<String, String> configuration, CheckpointStorage checkpointStorage) {
                return new CoordinatorCheckpointStorer(
                        checkpointStorage,
                        configuration.get(CheckpointStorer.Configuration.PATH)
                );
            }
        };

        public abstract CheckpointStorer newInstance(Map<String, String> configuration, CheckpointStorage checkpointStorage);
    }

    interface Configuration {
        String TYPE = "checkpoint.storer.type";
        String PATH = "checkpoint.storer.path";
    }

    static CheckpointStorer build(Map<String, String> configuration, CheckpointStorage checkpointStorage) {
        return CheckpointStorer.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration, checkpointStorage);
    }
}
