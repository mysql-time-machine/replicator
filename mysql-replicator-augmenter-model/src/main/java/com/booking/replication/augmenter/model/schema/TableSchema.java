package com.booking.replication.augmenter.model.schema;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class TableSchema implements Serializable {

    private FullTableName fullTableName;
    private Collection<ColumnSchema> columnSchemas;
    private String create;

    public TableSchema() {
    }

    public TableSchema(FullTableName fullTableName, Collection<ColumnSchema> columnSchemas, String create) {
        this.fullTableName = fullTableName;
        this.columnSchemas = columnSchemas;
        this.create = create;
    }

    public  Collection<ColumnSchema> getColumnSchemas() {
        return this.columnSchemas;
    }

    public List<String> getPrimaryKeyColumns() {
        return this
                .columnSchemas
                .stream()
                .filter(column -> column.isPrimary())
                .map(column -> column.getName())
                .collect(Collectors.toList());
    }

    public static List<String> getPrimaryKeyColumns(Collection<ColumnSchema> colSchemas) {
        return colSchemas.stream()
                .filter(column -> column.isPrimary())
                .map(column -> column.getName())
                .collect(Collectors.toList());
    }

    public String getCreate() {
        return this.create;
    }

    public FullTableName getFullTableName() {
        return this.fullTableName;
    }

    public void setFullTableName(FullTableName fullTableName) {
        this.fullTableName = fullTableName;
    }

    public void setColumnSchemas(Collection<ColumnSchema> columnSchemas) {
        this.columnSchemas = columnSchemas;
    }

    public void setCreate(String create) {
        this.create = create;
    }
}
