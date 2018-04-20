package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface QueryEventData extends EventData {

    // note: this is info from the binlog, not the current thread id
    long getThreadId();

    long getExecutionTime();

    int getErrorCode();

    String getDatabase();

    String getSQL();
}
