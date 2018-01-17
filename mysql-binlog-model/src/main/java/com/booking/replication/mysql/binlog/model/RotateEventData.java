package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface RotateEventData extends EventData {
    String getBinlogFilename();
    long getBinlogPosition();
}