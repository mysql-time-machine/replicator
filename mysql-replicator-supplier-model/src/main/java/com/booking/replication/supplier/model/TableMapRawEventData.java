package com.booking.replication.supplier.model;

import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;

import java.util.BitSet;

@SuppressWarnings("unused")
public interface TableMapRawEventData extends TableIdRawEventData {
    String getDatabase();

    String getTable();

    byte[] getColumnTypes();

    int[] getColumnMetadata();

    BitSet getColumnNullability();

    // TODO: replace TableMapEventMetadata with interface TableMapRawEventMetadata
    TableMapEventMetadata getEventMetadata();
}
