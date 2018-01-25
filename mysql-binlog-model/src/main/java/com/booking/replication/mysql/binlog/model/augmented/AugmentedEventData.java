package com.booking.replication.mysql.binlog.model.augmented;

import com.booking.replication.mysql.binlog.model.EventData;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public interface AugmentedEventData extends EventData {
    String getTableName();
    Map<String, Map<String, String>> getEventColumns();
    TableSchemaVersion getTableSchemaVersion();
    List<String> getPrimaryKeyColumns();
}
