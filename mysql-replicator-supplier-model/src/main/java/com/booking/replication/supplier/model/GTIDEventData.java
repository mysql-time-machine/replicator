package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface GTIDEventData extends EventData {
    String getGTID();

    byte getFlags();
}
