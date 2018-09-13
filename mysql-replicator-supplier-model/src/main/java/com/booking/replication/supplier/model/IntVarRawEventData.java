package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface IntVarRawEventData extends RawEventData {
    int getType();

    long getValue();
}
