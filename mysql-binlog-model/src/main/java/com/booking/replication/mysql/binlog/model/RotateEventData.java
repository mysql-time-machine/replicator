package com.booking.replication.mysql.binlog.model;

public interface RotateEventData extends EventData {
    String getBinlogFilename();
    long getBinlogPosition();
}