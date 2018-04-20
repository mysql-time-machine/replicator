package com.booking.replication.supplier.model;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

@SuppressWarnings("unused")
public interface WriteRowsEventData extends EventData {
    long getTableId();

    BitSet getIncludedColumns();

    List<Serializable[]> getRows();
}
