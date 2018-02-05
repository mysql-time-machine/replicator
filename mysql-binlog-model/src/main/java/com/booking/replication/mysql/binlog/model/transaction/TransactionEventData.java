package com.booking.replication.mysql.binlog.model.transaction;

import com.booking.replication.mysql.binlog.model.Event;
import com.booking.replication.mysql.binlog.model.augmented.TableNameEventData;

import java.util.List;
import java.util.UUID;

@SuppressWarnings("unused")
public interface TransactionEventData extends TableNameEventData {
    List<Event> getEvents();
    UUID getUuid();
    long getXid();
}
