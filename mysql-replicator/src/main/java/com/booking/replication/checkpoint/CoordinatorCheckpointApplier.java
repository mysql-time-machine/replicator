package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.CheckpointStorage;
import com.booking.replication.streams.Streams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CoordinatorCheckpointApplier implements CheckpointApplier {
    private final CheckpointStorage storage;
    private final String path;
    private final Map<Integer, Checkpoint> taskCheckpointMap;

    public CoordinatorCheckpointApplier(CheckpointStorage storage, String path) {
        this.storage = storage;
        this.path = path;
        this.taskCheckpointMap = new ConcurrentHashMap<>();
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent, Streams.Task task) {
        Checkpoint checkpoint = augmentedEvent.getHeader().getCheckpoint();

        if (checkpoint.getGTID() != null && checkpoint.getGTID().getValue() != null && checkpoint.getGTID().getIndex() == 0) {
            this.taskCheckpointMap.put(task.getCurrent(), checkpoint);

            if (this.taskCheckpointMap.size() == task.getTotal()) {
                Checkpoint minimumCheckpoint = checkpoint;

                for (Checkpoint taskCheckpoint : this.taskCheckpointMap.values()) {
                    if (minimumCheckpoint.compareTo(taskCheckpoint) < 0) {
                        minimumCheckpoint = taskCheckpoint;
                    }
                }

                try {
                    this.storage.saveCheckpoint(this.path, checkpoint);
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            }
        }
    }
}
