package com.booking.replication.mysql.binlog.model;

public interface XIDEventData extends EventData {
    long getXID();
}
