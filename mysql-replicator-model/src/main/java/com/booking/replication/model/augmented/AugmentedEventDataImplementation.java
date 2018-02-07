package com.booking.replication.model.augmented;

import java.util.List;
import java.util.Map;

public class AugmentedEventDataImplementation implements AugmentedEventData {
    private Map<String, Map<String, String>> eventColumns;
    private TableSchemaVersion tableSchemaVersion;
    private List<String> primaryKeyColumns;
    private String tableName;

    @Override
    public Map<String, Map<String, String>> getEventColumns() {
        return this.eventColumns;
    }

    public void setEventColumns(Map<String, Map<String, String>> eventColumns) {
        this.eventColumns = eventColumns;
    }

    @Override
    public TableSchemaVersion getTableSchemaVersion() {
        return this.tableSchemaVersion;
    }

    public void setTableSchemaVersion(TableSchemaVersion tableSchemaVersion) {
        this.tableSchemaVersion = tableSchemaVersion;
    }

    @Override
    public List<String> getPrimaryKeyColumns() {
        return this.primaryKeyColumns;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
