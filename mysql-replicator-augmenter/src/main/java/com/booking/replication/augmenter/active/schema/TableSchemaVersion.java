package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.column.ColumnSchema;
import com.booking.replication.commons.util.CaseInsensitiveMap;

import java.util.*;


public class TableSchemaVersion {

    private final Map<String,ColumnSchema> columns = new CaseInsensitiveMap<>();

    private final Map<Integer,String> columnIndexToColumnNameMap = new HashMap<>();

    private final String tableSchemaVersionUUID;

    // TODO: load table CHARACTER_SET_NAME
    // private String CHARACTER_SET_NAME;

    public TableSchemaVersion() {
        tableSchemaVersionUUID = UUID.randomUUID().toString();;
    }

    public void addColumn(ColumnSchema columnSchema) {
        String columnName = columnSchema.getColumnName();
        this.columns.put(columnName, columnSchema);

        // update the indexToNameMap
        Integer index = columnSchema.getOrdinalPosition();
        columnIndexToColumnNameMap.put(index, columnName);
    }

    public ColumnSchema getColumnSchemaByColumnName(String columnName) {
        return this.columns.get(columnName);
    }

    public ColumnSchema getColumnSchemaByColumnIndex(Integer columnIndex) {
        String columnName = columnIndexToColumnNameMap.get(columnIndex);
        return columns.get(columnName);
    }

    public Set<String> getColumnNames() {
        return Collections.unmodifiableSet(columns.keySet());
    }
}