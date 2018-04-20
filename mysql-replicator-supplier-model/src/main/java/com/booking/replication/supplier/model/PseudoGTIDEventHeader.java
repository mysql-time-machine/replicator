package com.booking.replication.supplier.model;

public interface PseudoGTIDEventHeader extends EventHeaderV4 {
    Checkpoint getCheckpoint();
}
