package com.booking.replication.coordinator;

import java.io.IOException;

public interface CheckpointCoordinator {
    <Type> void storeCheckpoint(String path, Type checkpoint) throws IOException;
    <Type> Type loadCheckpoint(String path, Class<Type> type) throws IOException;
    String defaultCheckpointPath();
}
