package com.booking.replication.coordinator;

import java.io.IOException;

public interface CheckpointCoordinator {
    void storeCheckpoint(String path, byte[] checkpoint) throws IOException;
    byte[] loadCheckpoint(String path) throws IOException;
    String defaultCheckpointPath();
}
