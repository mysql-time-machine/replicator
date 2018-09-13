package com.booking.replication.supplier.model;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

@SuppressWarnings("unused")
public interface DeleteRowsRawEventData extends TableIdRawEventData {
    BitSet getIncludedColumns();

    List<Serializable[]> getRows();
}
