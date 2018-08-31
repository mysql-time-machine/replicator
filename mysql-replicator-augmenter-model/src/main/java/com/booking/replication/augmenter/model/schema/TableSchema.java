package com.booking.replication.augmenter.model.schema;

import java.io.Serializable;
import java.util.Collection;

@SuppressWarnings("unused")
public class TableSchema implements Serializable {
    private Collection<ColumnSchema> columns;
    private String create;

    public TableSchema() {
    }

    public TableSchema(Collection<ColumnSchema> columns, String create) {
        this.columns = columns;
        this.create = create;
    }

    public  Collection<ColumnSchema> getColumns() {
        return this.columns;
    }

    public String getCreate() {
        return this.create;
    }
}
