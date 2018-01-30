package com.booking.replication.mysql.binlog.model.transaction;

import com.booking.replication.mysql.binlog.model.Event;
import com.booking.replication.mysql.binlog.model.augmented.TableEventData;

import java.util.List;
import java.util.UUID;

@SuppressWarnings("unused")
public interface TransactionEventData extends TableEventData {
    List<Event> getEvents();
    UUID getUuid();
    long getXid();
}
