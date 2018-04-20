package com.booking.replication.model;

@SuppressWarnings("unused")
public interface XAPrepareEventData extends EventData {
    boolean isOnePhase();

    int getFormatID();

    int getGTRIDLength();

    int getBQUALLength();

    byte[] getData();
}
