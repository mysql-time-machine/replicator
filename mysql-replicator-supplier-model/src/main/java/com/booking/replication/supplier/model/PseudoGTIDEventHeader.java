package com.booking.replication.supplier.model;

import com.booking.replication.supplier.model.checkpoint.Checkpoint;

public interface PseudoGTIDEventHeader extends EventHeaderV4 {
    Checkpoint getCheckpoint();
}
