package com.booking.replication.model;

@SuppressWarnings("unused")
public interface GTIDEventData extends EventData {
    String getGTID();
    byte getFlags();
}
