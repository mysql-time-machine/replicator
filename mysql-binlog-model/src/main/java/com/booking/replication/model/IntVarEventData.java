package com.booking.replication.model;

@SuppressWarnings("unused")
public interface IntVarEventData extends EventData {
    int getType();
    long getValue();
}
