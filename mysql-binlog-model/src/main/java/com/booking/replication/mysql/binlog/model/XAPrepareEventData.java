package com.booking.replication.mysql.binlog.model;

public interface XAPrepareEventData extends EventData {
    boolean isOnePhase();
    int getFormatID();
    int getGTRIDLength();
    int getBQUALLength();
    byte[] getData();
}
