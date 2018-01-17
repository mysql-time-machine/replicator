package com.booking.replication.mysql.binlog.model;

public interface IntVarEventData extends EventData {
    int getType();
    long getValue();
}
