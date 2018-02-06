package com.booking.replication.model.transaction;

import com.booking.replication.model.Event;
import com.booking.replication.model.augmented.TableNameEventData;

import java.util.List;
import java.util.UUID;

@SuppressWarnings("unused")
public interface TransactionEventData extends TableNameEventData {
    List<Event> getEvents();
    UUID getUuid();
    long getXid();
}
