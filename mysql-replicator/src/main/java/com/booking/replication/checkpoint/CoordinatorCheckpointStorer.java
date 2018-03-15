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
    private final AtomicLong currentServerID;
    private final AtomicReference<String> currentBinlogFilename;
    private final AtomicLong currentBinlogPosition;

    public CoordinatorCheckpointStorer(CheckpointCoordinator coordinator, String checkpointPath) {
        this.coordinator = coordinator;
        this.checkpointPath = checkpointPath;
        this.currentServerID = new AtomicLong();
        this.currentBinlogFilename = new AtomicReference<>();
        this.currentBinlogPosition = new AtomicLong();
    }

    @Override
    public void accept(Event event) {
        AugmentedEventHeader eventHeader = AugmentedEventHeader.class.cast(event.getHeader());

        if (eventHeader.getEventType() == EventType.ROTATE) {
            RotateEventData eventData = RotateEventData.class.cast(event.getData());

            this.currentServerID.set(eventHeader.getServerId());
            this.currentBinlogFilename.set(eventData.getBinlogFilename());
            this.currentBinlogPosition.set(eventData.getBinlogPosition());
        }

        if (eventHeader.getPseudoGTIDIndex() == 0) {
            try {
                this.coordinator.storeCheckpoint(
                        this.checkpointPath,
                        new Checkpoint(
                                this.currentServerID.get(),
                                this.currentBinlogFilename.get(),
                                this.currentBinlogPosition.get(),
                                eventHeader.getPseudoGTID(),
                                eventHeader.getPseudoGTIDIndex()
                        )
                );
            } catch (IOException exception) {
                throw new UncheckedIOException(exception);
            }
        }
    }
}
