package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface FormatDescriptionEventData extends EventData {
    int getBinlogVersion();
    String getServerVersion();
    int getHeaderLength();
}
