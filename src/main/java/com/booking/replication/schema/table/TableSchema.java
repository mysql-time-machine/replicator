package com.booking.replication.schema.table;

import com.booking.replication.schema.column.ColumnSchema;

import java.util.HashMap;


public class TableSchema {

    private HashMap<String,ColumnSchema> columns;

    private HashMap<Integer,String> columnIndexToColumnNameMap = new HashMap<Integer,String>();

    // TODO: load table CHARACTER_SET_NAME
    private String CHARACTER_SET_NAME;

    public TableSchema() {
        columns = new HashMap<String, ColumnSchema>();
    }

    public void addColumn(ColumnSchema columnSchema)
    {
        this.columns.put(columnSchema.getCOLUMN_NAME(), columnSchema);

        // update the indexToNameMap
        Integer index = columnSchema.getORDINAL_POSITION();
        String  name  = columnSchema.getCOLUMN_NAME();
        columnIndexToColumnNameMap.put(index,name);
    }

    public ColumnSchema getColumnSchemaByColumnName(String columnName) {
        ColumnSchema c = this.columns.get(columnName);
        return c;
    }

    public ColumnSchema getColumnSchemaByColumnIndex(Integer columnIndex) {
        String columnName = this.getColumnIndexToNameMap().get(columnIndex);
        ColumnSchema c = this.columns.get(columnName);
        return c;
    }

    public HashMap<String, ColumnSchema> getColumnsSchema() {
        return columns;
    }

    public HashMap<Integer,String> getColumnIndexToNameMap () {
        return columnIndexToColumnNameMap;
    }
}
