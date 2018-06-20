package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.CheckpointStorage;
import com.booking.replication.streams.Streams;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

public interface CheckpointApplier extends BiConsumer<AugmentedEvent, Streams.Task>, Closeable {
    enum Type {
        NONE {
            @Override
            protected CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath, long period) {
                return (event, map) -> {
                };
            }
        },
        COORDINATOR {
            @Override
            protected CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath, long period) {
                return new CoordinatorCheckpointApplier(checkpointStorage, checkpointPath, period);
            }
        };

        protected abstract CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath, long period);
    }

    interface Configuration {
        String TYPE = "checkpoint.applier.type";
        String PERIOD = "checkpoint.applier.period.ms";
    }

    @Override
    default void close() throws IOException {
    }

    static CheckpointApplier build(Map<String, Object> configuration, CheckpointStorage checkpointStorage, String checkpointPath) {
        return CheckpointApplier.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name()).toString()
        ).newInstance(
                checkpointStorage,
                checkpointPath,
                Long.parseLong(configuration.getOrDefault(Configuration.PERIOD, "5000").toString())
        );
    }
}
