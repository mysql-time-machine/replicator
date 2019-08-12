package com.booking.replication.augmenter.model.event;

import com.booking.replication.augmenter.model.row.AugmentedRow;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.augmenter.model.schema.FullTableName;

import java.util.Collection;

@SuppressWarnings("unused")
public class DeleteRowsAugmentedEventData extends RowsAugmentedEventData {

    public DeleteRowsAugmentedEventData() { }

    public DeleteRowsAugmentedEventData(
            FullTableName eventTable,
            Collection<Boolean> includedColumns,
            Collection<ColumnSchema> columns,
            Collection<AugmentedRow> augmentedRows) {

        super(eventTable, includedColumns, columns, augmentedRows);
    }
}
