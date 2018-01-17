package com.booking.replication.mysql.binlog.model;

public interface RowsQueryEventData extends EventData {
    String getQuery();
}
