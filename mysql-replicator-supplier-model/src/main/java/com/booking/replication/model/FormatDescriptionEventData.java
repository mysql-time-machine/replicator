package com.booking.replication.model;

@SuppressWarnings("unused")
public interface FormatDescriptionEventData extends EventData {
    int getBinlogVersion();

    String getServerVersion();

    int getHeaderLength();
}
