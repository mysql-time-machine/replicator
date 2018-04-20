package com.booking.replication.model;

@SuppressWarnings("unused")
public interface XIDEventData extends EventData {
    long getXID();
}
