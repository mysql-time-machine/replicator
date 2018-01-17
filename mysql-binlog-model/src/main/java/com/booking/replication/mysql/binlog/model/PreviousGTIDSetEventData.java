package com.booking.replication.mysql.binlog.model;

public interface PreviousGTIDSetEventData extends EventData {
    String getGTIDSet();
}
