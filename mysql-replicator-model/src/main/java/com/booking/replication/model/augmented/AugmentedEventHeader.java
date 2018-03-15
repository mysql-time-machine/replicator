package com.booking.replication.model.augmented;

import com.booking.replication.model.EventHeaderV4;

public interface AugmentedEventHeader extends EventHeaderV4 {
    String getPseudoGTID();
    int getPseudoGTIDIndex();
}
