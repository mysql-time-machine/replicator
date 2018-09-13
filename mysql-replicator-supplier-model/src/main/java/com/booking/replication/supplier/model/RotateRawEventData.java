package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface RotateRawEventData extends RawEventData {
    String getBinlogFilename();

    long getBinlogPosition();
}