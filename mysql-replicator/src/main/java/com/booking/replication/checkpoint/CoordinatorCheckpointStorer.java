package com.booking.replication.checkpoint;

import com.booking.replication.coordinator.CheckpointCoordinator;
import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.RawEvent;
import com.booking.replication.model.PseudoGTIDEventHeader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CoordinatorCheckpointStorer implements CheckpointStorer {
    private final CheckpointCoordinator coordinator;
    private final String checkpointPath;

    public CoordinatorCheckpointStorer(CheckpointCoordinator coordinator, String checkpointPath) {
        this.coordinator = coordinator;
        this.checkpointPath = checkpointPath;
    }

    @Override
    public void accept(RawEvent rawEvent, Map<RawEvent, AtomicReference<RawEvent>> executing) {
        Checkpoint checkpoint = PseudoGTIDEventHeader.class.cast(rawEvent.getHeader()).getCheckpoint();

        if (checkpoint.getPseudoGTID() != null && checkpoint.getPseudoGTIDIndex() == 0) {
            /*if (executing.keySet().stream().map(RawEvent::getHeader).allMatch(
                    eventHeader -> checkpoint.compareTo(PseudoGTIDEventHeader.class.cast(eventHeader).getCheckpoint()) >= 0
            )) {*/
                try {
                    this.coordinator.storeCheckpoint(this.checkpointPath, checkpoint);
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            //}
        }
    }
}
