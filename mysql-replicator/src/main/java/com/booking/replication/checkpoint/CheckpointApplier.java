package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.CheckpointStorage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public interface CheckpointApplier extends BiConsumer<AugmentedEvent, Map<AugmentedEvent, AtomicReference<AugmentedEvent>>>, Closeable {
    enum Type {
        NONE {
            @Override
            protected CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath) {
                return (event, map) -> {
                };
            }
        },
        COORDINATOR {
            @Override
            protected CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath) {
                return new CoordinatorCheckpointApplier(checkpointStorage, checkpointPath);
            }
        };

        protected abstract CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath);
    }

    interface Configuration {
        String TYPE = "checkpoint.applier.type";
    }

    @Override
    default void close() throws IOException {
    }

    static CheckpointApplier build(Map<String, Object> configuration, CheckpointStorage checkpointStorage, String checkpointPath) {
        return CheckpointApplier.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name()).toString()
        ).newInstance(checkpointStorage, checkpointPath);
    }
}
