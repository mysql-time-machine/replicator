package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface XIDEventData extends EventData {
    long getXID();
}
