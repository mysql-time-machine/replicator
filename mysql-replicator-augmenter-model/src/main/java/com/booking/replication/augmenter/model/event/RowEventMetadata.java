package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.augmenter.model.schema.TableSchema;

import java.util.Collection;
import java.util.List;

public class RowEventMetadata extends EventMetadata {

    private Collection<ColumnSchema> columns;
    private List<String> primaryKeyColumns;

    public  RowEventMetadata() {

    }

    public RowEventMetadata(FullTableName eventTable, AugmentedEventType eventType, Collection<ColumnSchema> columns) {
        super(eventTable, eventType);
        this.columns    = columns;

        primaryKeyColumns = TableSchema.getPrimaryKeyColumns(columns);
    }

    public Collection<ColumnSchema> getColumns() {
        return columns;
    }

    public List<String> getPrimaryKeyColumns() {
        return this.primaryKeyColumns;
    }

    public void setColumns(Collection<ColumnSchema> columns) {
        this.columns = columns;
    }

    public void setPrimaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }
}
