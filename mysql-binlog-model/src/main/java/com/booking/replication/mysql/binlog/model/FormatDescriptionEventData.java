package com.booking.replication.mysql.binlog.model;

public interface FormatDescriptionEventData extends EventData {
    int getBinlogVersion();
    String getServerVersion();
    int getHeaderLength();
}
