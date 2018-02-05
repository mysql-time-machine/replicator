package com.booking.replication.model.augmented;

import java.util.Set;

@SuppressWarnings("unused")
public interface TableSchemaVersion {
    ColumnSchema getColumnSchemaByColumnName(String columnName);
    ColumnSchema getColumnSchemaByColumnIndex(Integer columnIndex);
    Set<String> getColumnNames();
}
