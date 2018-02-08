package com.booking.replication.model.augmented.active.schema;

import java.util.Set;

@SuppressWarnings("unused")
public interface TableSchemaVersion {
    ColumnSchema getColumnSchemaByColumnName(String columnName);
    ColumnSchema getColumnSchemaByColumnIndex(int columnIndex);
    Set<String> getColumnNames();
}
