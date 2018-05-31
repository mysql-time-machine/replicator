package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.commons.checkpoint.CheckpointStorage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CoordinatorCheckpointApplier implements CheckpointApplier {
    private final CheckpointStorage storage;
    private final String path;

    public CoordinatorCheckpointApplier(CheckpointStorage storage, String path) {
        this.storage = storage;
        this.path = path;
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent, Map<AugmentedEvent, AtomicReference<AugmentedEvent>> executing) {
        Checkpoint checkpoint = augmentedEvent.getHeader().getCheckpoint();

        if (checkpoint.getPseudoGTID() != null && checkpoint.getPseudoGTIDIndex() == 0) {
            /*if (executing.keySet().stream().map(RawEvent::getHeader).allMatch(
                    eventHeader -> checkpoint.compareTo(PseudoGTIDEventHeader.class.cast(eventHeader).getCheckpoint()) >= 0
            )) {*/
                try {
                    this.storage.saveCheckpoint(this.path, checkpoint);
                } catch (IOException exception) {
                    throw new UncheckedIOException(exception);
                }
            //}
        }
    }
}
