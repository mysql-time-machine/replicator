package com.booking.replication.augmenter.transaction;

import com.booking.replication.model.Event;
import com.booking.replication.model.TableNameEventData;

import java.util.List;
import java.util.UUID;

@SuppressWarnings("unused")
public interface TransactionEventData extends TableNameEventData {
    List<Event> getEvents();

    UUID getUuid();

    long getXid();
}
