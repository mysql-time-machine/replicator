package com.booking.replication.model;

import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.EventHeaderV4;

public interface PseudoGTIDEventHeader extends EventHeaderV4 {
    Checkpoint getCheckpoint();
}
