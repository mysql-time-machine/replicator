package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;

@SuppressWarnings("unused")
public class UpdateRowsAugmentedEventData extends RowsAugmentedEventData {

    @JsonIgnore
    private Collection<Boolean> includedColumnsBeforeUpdate;

    public UpdateRowsAugmentedEventData() { }

    public UpdateRowsAugmentedEventData(
            FullTableName eventTable,
            Collection<Boolean> includedColumnsBeforeUpdate,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> augmentedRows
    ) {
        super(eventTable, includedColumns, columns, augmentedRows);

        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
    }

    public Collection<Boolean> getIncludedColumnsBeforeUpdate() {
        return this.includedColumnsBeforeUpdate;
    }

    public void setIncludedColumnsBeforeUpdate(Collection<Boolean> includedColumnsBeforeUpdate) {
        this.includedColumnsBeforeUpdate = includedColumnsBeforeUpdate;
    }
}
