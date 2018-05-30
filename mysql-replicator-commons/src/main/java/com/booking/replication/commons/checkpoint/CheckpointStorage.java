package com.booking.replication.commons.checkpoint;

import java.io.IOException;

public interface CheckpointStorage {
    void storeCheckpoint(String path, Checkpoint checkpoint) throws IOException;

    Checkpoint loadCheckpoint(String path) throws IOException;
}
