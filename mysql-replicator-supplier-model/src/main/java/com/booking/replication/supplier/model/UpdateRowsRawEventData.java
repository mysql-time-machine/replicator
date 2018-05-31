package com.booking.replication.supplier.model;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public interface UpdateRowsRawEventData extends TableIdRawEventData {
    BitSet getIncludedColumnsBeforeUpdate();

    BitSet getIncludedColumns();

    List<Map.Entry<Serializable[], Serializable[]>> getRows();
}
