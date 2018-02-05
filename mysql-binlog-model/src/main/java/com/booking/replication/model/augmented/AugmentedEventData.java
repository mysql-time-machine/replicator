package com.booking.replication.model.augmented;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public interface AugmentedEventData extends TableNameEventData {
    Map<String, Map<String, String>> getEventColumns();
    TableSchemaVersion getTableSchemaVersion();
    List<String> getPrimaryKeyColumns();
}
