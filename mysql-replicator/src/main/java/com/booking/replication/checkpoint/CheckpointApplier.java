package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.AugmenterContext;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.CheckpointStorage;

import java.io.Closeable;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface CheckpointApplier extends BiConsumer<AugmentedEvent, Integer>, Closeable {
    enum Type {
        NONE {
            @Override
            protected CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath, long period, boolean transactionEnabled, Consumer<Checkpoint> safeCheckpointCallback) {
                return new DummyCheckPointApplier();
            }
        },
        COORDINATOR {
            @Override
            protected CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath, long period, boolean transactionEnabled, Consumer<Checkpoint> safeCheckpointCallback) {
                return new CoordinatorCheckpointApplier(checkpointStorage, checkpointPath, period, transactionEnabled, safeCheckpointCallback);
            }
        };

        protected abstract CheckpointApplier newInstance(CheckpointStorage checkpointStorage, String checkpointPath, long period, boolean transactionEnabled, Consumer<Checkpoint> safeCheckpointCallback);
    }

    interface Configuration {
        String TYPE = "checkpoint.applier.type";
        String PERIOD = "checkpoint.applier.period.ms";
    }

    @Override
    default void close() {
    }

    static CheckpointApplier build(Map<String, Object> configuration, CheckpointStorage checkpointStorage, String checkpointPath, Consumer<Checkpoint> safeCheckpointCallback) {
        boolean transactionEnabled = Boolean.parseBoolean(configuration.getOrDefault(AugmenterContext.Configuration.TRANSACTIONS_ENABLED, "true").toString());
        return CheckpointApplier.Type.valueOf(
                configuration.getOrDefault(Configuration.TYPE, Type.NONE.name()).toString()
        ).newInstance(
                checkpointStorage,
                checkpointPath,
                Long.parseLong(configuration.getOrDefault(Configuration.PERIOD, "5000").toString()),
                transactionEnabled,
                safeCheckpointCallback
        );
    }
}
