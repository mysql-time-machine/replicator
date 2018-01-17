package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface XIDRawEventData extends RawEventData {
    long getXID();
}
