package com.booking.replication.mysql.binlog.model;

@SuppressWarnings("unused")
public interface QueryEventData extends EventData {
    long getThreadId();
    long getExecutionTime();
    int getErrorCode();
    String getDatabase();
    String getSQL();
}
