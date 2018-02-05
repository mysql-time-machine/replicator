package com.booking.replication.mysql.binlog.model.augmented;

import com.booking.replication.mysql.binlog.model.EventData;

@SuppressWarnings("unused")
public interface TableNameEventData extends EventData {
    String getTableName();
}
