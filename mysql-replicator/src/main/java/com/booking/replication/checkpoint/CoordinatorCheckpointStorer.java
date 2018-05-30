package com.booking.replication.checkpoint;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.commons.checkpoint.CheckpointStorage;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class CoordinatorCheckpointStorer implements CheckpointStorer {
    private final CheckpointStorage checkpointStorage;
    private final String checkpointPath;

    public CoordinatorCheckpointStorer( CheckpointStorage checkpointStorage, String checkpointPath) {
        this.checkpointStorage = checkpointStorage;
        this.checkpointPath = checkpointPath;
    }

    @Override
    public void accept(AugmentedEvent augmentedEvent, Map<AugmentedEvent, AtomicReference<AugmentedEvent>> executing) {
//        Checkpoint checkpoint = PseudoGTIDEventHeader.class.cast(augmentedEvent.getHeader()).getCheckpoint();
//
//        if (checkpoint.getPseudoGTID() != null && checkpoint.getPseudoGTIDIndex() == 0) {
//            /*if (executing.keySet().stream().map(RawEvent::getHeader).allMatch(
//                    eventHeader -> checkpoint.compareTo(PseudoGTIDEventHeader.class.cast(eventHeader).getCheckpoint()) >= 0
//            )) {*/
//                try {
//                    this.coordinator.storeCheckpoint(this.checkpointPath, checkpoint);
//                } catch (IOException exception) {
//                    throw new UncheckedIOException(exception);
//                }
//            //}
//        }
    }
}
