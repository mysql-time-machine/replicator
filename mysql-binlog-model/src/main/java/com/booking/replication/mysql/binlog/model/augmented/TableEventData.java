package com.booking.replication.mysql.binlog.model.augmented;

import com.booking.replication.mysql.binlog.model.EventData;

@SuppressWarnings("unused")
public interface TableEventData extends EventData {
    String getTableName();
}
