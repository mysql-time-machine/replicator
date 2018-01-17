package com.booking.replication.mysql.binlog.model;

import java.util.BitSet;

public interface TableMapEventData extends EventData {
    long getTableId();
    String getDatabase();
    String getTable();
    byte[] getColumnTypes();
    int[] getColumnMetadata();
    BitSet getColumnNullability();
}
