package com.booking.replication.model;

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
