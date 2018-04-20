package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface RowsQueryEventData extends EventData {
    String getQuery();
}
