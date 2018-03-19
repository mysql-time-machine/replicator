package com.booking.replication.model;

@SuppressWarnings("unused")
public interface RotateEventData extends EventData {
    String getBinlogFilename();

    long getBinlogPosition();
}