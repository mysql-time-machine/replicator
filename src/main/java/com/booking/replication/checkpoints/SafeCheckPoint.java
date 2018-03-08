package com.booking.replication.checkpoints;

import java.io.Serializable;

public interface SafeCheckPoint extends Serializable {

    public int getCheckpointType();

    public boolean isBeforeCheckpoint(PseudoGTIDCheckpoint checkpointToCompareTo);

    public String toJson();
}
