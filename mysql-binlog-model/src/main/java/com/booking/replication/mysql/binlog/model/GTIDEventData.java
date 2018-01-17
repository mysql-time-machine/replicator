package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface GTIDEventData extends EventData {
    String getGTID();
    byte getFlags();
}
