package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface FormatDescriptionEventData extends EventData {
    int getBinlogVersion();

    String getServerVersion();

    int getHeaderLength();
}
