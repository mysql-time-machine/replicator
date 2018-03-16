package com.booking.replication.checkpoint;

import com.booking.replication.coordinator.CheckpointCoordinator;
import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.model.EventType;
import com.booking.replication.model.augmented.AugmentedEventHeader;
import com.github.shyiko.mysql.binlog.event.RotateEventData;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class CoordinatorCheckpointStorer implements CheckpointStorer {
    private final CheckpointCoordinator coordinator;
    private final String checkpointPath;

    public CoordinatorCheckpointStorer(CheckpointCoordinator coordinator, String checkpointPath) {
        this.coordinator = coordinator;
        this.checkpointPath = checkpointPath;
    }

    @Override
    public void accept(Event event) {
        Checkpoint checkpoint = AugmentedEventHeader.class.cast(event.getHeader()).getCheckpoint();

        if (checkpoint.getPseudoGTID() != null && checkpoint.getPseudoGTIDIndex() == 0) {
            try {
                this.coordinator.storeCheckpoint(this.checkpointPath, checkpoint);
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }
    }
}
