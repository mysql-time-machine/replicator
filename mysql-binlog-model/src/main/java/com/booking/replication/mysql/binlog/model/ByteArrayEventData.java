package com.booking.replication.mysql.binlog.model;

public interface ByteArrayEventData extends EventData {
    byte[] getData();
}
