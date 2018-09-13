package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface GTIDRawEventData extends RawEventData {
    String getGTID();

    byte getFlags();
}
