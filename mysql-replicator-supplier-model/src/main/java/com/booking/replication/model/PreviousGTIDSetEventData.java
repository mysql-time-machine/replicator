package com.booking.replication.model;

@SuppressWarnings("unused")
public interface PreviousGTIDSetEventData extends EventData {
    String getGTIDSet();
}
