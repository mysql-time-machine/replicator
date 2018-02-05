package com.booking.replication.model;

@SuppressWarnings("unused")
public interface QueryEventData extends EventData {
    long getThreadId();
    long getExecutionTime();
    int getErrorCode();
    String getDatabase();
    String getSQL();
}
