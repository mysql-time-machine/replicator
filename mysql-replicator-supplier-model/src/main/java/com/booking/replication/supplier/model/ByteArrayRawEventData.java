package com.booking.replication.supplier.model;

@SuppressWarnings("unused")
public interface ByteArrayRawEventData extends RawEventData {
    byte[] getData();
}
