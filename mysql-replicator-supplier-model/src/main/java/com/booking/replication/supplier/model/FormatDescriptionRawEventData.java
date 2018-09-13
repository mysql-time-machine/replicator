package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface FormatDescriptionRawEventData extends RawEventData {
    int getBinlogVersion();

    String getServerVersion();

    int getHeaderLength();
}
