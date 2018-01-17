package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface IntVarEventData extends EventData {
    int getType();
    long getValue();
}
