package com.booking.replication.augmenter.model.schema;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
public class TableSchema implements Serializable {

    private FullTableName fullTableName;
    private Collection<ColumnSchema> columns;
    private String create;

    public TableSchema(FullTableName fullTableName, Collection<ColumnSchema> columns, String create) {
        this.fullTableName = fullTableName;
        this.columns = columns;
        this.create = create;
    }

    public  Collection<ColumnSchema> getColumns() {
        return this.columns;
    }

    public List<String> getPrimaryKeyColumns() {
        return this
                .columns
                .stream()
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
}
