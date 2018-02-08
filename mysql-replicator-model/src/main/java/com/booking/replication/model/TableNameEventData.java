package com.booking.replication.model.augmented;

import com.booking.replication.model.EventData;

@SuppressWarnings("unused")
public interface TableNameEventData extends EventData {
    String getTableName();
}
