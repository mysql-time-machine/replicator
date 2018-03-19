package com.booking.replication.model.augmented;

import com.booking.replication.model.augmented.active.schema.TableSchemaVersion;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public interface AugmentedRow {
    Map<String, Map<String, String>> getEventColumns();

    TableSchemaVersion getTableSchemaVersion();

    List<String> getPrimaryKeyColumns();
}
