package com.booking.replication.model.augmented;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;
import java.util.Set;

public class TableSchemaVersionImplementation implements TableSchemaVersion {
    private Map<String, ColumnSchema> columns;

    @Override
    @JsonIgnore
    public ColumnSchema getColumnSchemaByColumnName(String columnName) {
        return this.columns.get(columnName);
    }

    @Override
    @JsonIgnore
    public ColumnSchema getColumnSchemaByColumnIndex(int columnIndex) {
        int index = 0;

        for (String key : this.columns.keySet()) {
            if (index < columnIndex) {
                index++;
            } else {
                return this.columns.get(key);
            }
        }

        return null;
    }

    @Override
    @JsonIgnore
    public Set<String> getColumnNames() {
        return this.columns.keySet();
    }

    public Map<String, ColumnSchema> getColumns() {
        return this.columns;
    }

    public void setColumns(Map<String, ColumnSchema> columns) {
        this.columns = columns;
    }
}
