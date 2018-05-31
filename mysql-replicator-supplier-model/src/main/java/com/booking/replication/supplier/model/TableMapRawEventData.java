package com.booking.replication.supplier.model;

import java.util.BitSet;

@SuppressWarnings("unused")
public interface TableMapRawEventData extends TableIdRawEventData {
    String getDatabase();

    String getTable();

    byte[] getColumnTypes();

    int[] getColumnMetadata();

    BitSet getColumnNullability();
}
