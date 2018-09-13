package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface XAPrepareRawEventData extends RawEventData {
    boolean isOnePhase();

    int getFormatID();

    int getGTRIDLength();

    int getBQUALLength();

    byte[] getData();
}
