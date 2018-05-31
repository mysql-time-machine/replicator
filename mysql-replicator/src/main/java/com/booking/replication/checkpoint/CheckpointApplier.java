package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.CheckpointStorage;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public interface CheckpointApplier extends BiConsumer<AugmentedEvent, Map<AugmentedEvent, AtomicReference<AugmentedEvent>>> {
    enum Type {
        NONE {
            @Override
            public CheckpointApplier newInstance(Map<String, String> configuration, CheckpointStorage checkpointStorage) {
                return (event, map) -> {
                };
            }
        },
        COORDINATOR {
            @Override
            public CheckpointApplier newInstance(Map<String, String> configuration, CheckpointStorage checkpointStorage) {
                return new CoordinatorCheckpointApplier(
                        checkpointStorage,
                        configuration.get(CheckpointApplier.Configuration.PATH)
                );
            }
        };

        public abstract CheckpointApplier newInstance(Map<String, String> configuration, CheckpointStorage checkpointStorage);
    }

    interface Configuration {
        String TYPE = "checkpoint.applier.type";
        String PATH = "checkpoint.applier.path";
    }

    static CheckpointApplier build(Map<String, String> configuration, CheckpointStorage checkpointStorage) {
        return CheckpointApplier.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name())
        ).newInstance(configuration, checkpointStorage);
    }
}
