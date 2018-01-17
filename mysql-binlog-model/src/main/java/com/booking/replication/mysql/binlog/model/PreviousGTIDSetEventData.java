package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface PreviousGTIDSetEventData extends EventData {
    String getGTIDSet();
}
