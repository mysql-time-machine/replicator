package com.booking.replication.model;

@SuppressWarnings("unused")
public interface RowsQueryEventData extends EventData {
    String getQuery();
}
