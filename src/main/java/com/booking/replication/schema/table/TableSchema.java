package com.booking.replication.schema.table;

import com.booking.replication.schema.column.ColumnSchema;

import java.util.HashMap;


public class TableSchema {

    private HashMap<String,ColumnSchema> columns;

    private HashMap<Integer,String> columnIndexToColumnNameMap = new HashMap<>();

    // TODO: load table CHARACTER_SET_NAME
//    private String CHARACTER_SET_NAME;

    public TableSchema() {
        columns = new HashMap<>();
    }

    public void addColumn(ColumnSchema columnSchema) {
        this.columns.put(columnSchema.getColumnName(), columnSchema);

        // update the indexToNameMap
        Integer index = columnSchema.getOrdinalPosition();
        String  name  = columnSchema.getColumnName();
        columnIndexToColumnNameMap.put(index,name);
    }

    public ColumnSchema getColumnSchemaByColumnName(String columnName) {
        return this.columns.get(columnName);
    }

    public ColumnSchema getColumnSchemaByColumnIndex(Integer columnIndex) {
        String columnName = getColumnIndexToNameMap().get(columnIndex);
        return columns.get(columnName);
    }

    public HashMap<String, ColumnSchema> getColumnsSchema() {
        return columns;
    }

    public HashMap<Integer,String> getColumnIndexToNameMap () {
        return columnIndexToColumnNameMap;
    }
}
