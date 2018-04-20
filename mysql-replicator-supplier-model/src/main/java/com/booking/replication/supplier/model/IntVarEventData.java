package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface IntVarEventData extends EventData {
    int getType();

    long getValue();
}
