package com.booking.replication.mysql.binlog.model;

public interface GTIDEventData extends EventData {
    String getGTID();
    byte getFlags();
}
