package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface ByteArrayEventData extends EventData {
    byte[] getData();
}
