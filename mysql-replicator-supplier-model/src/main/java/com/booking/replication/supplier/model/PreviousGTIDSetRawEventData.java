package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface PreviousGTIDSetRawEventData extends RawEventData {
    String getGTIDSet();
}
