package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface RowsQueryEventData extends EventData {
    String getQuery();
}
