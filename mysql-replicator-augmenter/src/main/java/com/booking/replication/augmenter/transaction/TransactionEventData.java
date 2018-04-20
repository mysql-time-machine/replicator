package com.booking.replication.augmenter.transaction;

import com.booking.replication.model.RawEvent;
import com.booking.replication.model.TableNameEventData;

import java.util.List;
import java.util.UUID;

@SuppressWarnings("unused")
public interface TransactionEventData extends TableNameEventData {
    List<RawEvent> getRawEvents();

    UUID getUuid();

    long getXid();
}
