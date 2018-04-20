package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface RotateEventData extends EventData {
    String getBinlogFilename();

    long getBinlogPosition();
}