package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface QueryRawEventData extends RawEventData {
    long getThreadId();

    long getExecutionTime();

    int getErrorCode();

    String getDatabase();

    String getSQL();
}
