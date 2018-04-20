package com.booking.replication.supplier.model;

import java.util.BitSet;

@SuppressWarnings("unused")
public interface TableMapEventData extends EventData {
    long getTableId();

    String getDatabase();

    String getTable();

    byte[] getColumnTypes();

    int[] getColumnMetadata();

    BitSet getColumnNullability();
}
