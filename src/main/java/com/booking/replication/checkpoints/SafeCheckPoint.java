package com.booking.replication.checkpoints;

import java.io.Serializable;

public interface SafeCheckPoint extends Serializable {

    public boolean isBeforeCheckpoint(PseudoGTIDCheckpoint checkpointToCompareTo);

    public String toJson();
}
