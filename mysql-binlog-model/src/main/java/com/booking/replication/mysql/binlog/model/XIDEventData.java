package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface XIDEventData extends EventData {
    long getXID();
}
