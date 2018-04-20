package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface PreviousGTIDSetEventData extends EventData {
    String getGTIDSet();
}
