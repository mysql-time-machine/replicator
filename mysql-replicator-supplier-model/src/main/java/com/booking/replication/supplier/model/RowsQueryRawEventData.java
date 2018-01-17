package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface RowsQueryRawEventData extends RawEventData {
    String getQuery();
}
