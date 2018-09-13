package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.event.AugmentedEventData;
import com.booking.replication.augmenter.model.event.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventData;
import com.booking.replication.augmenter.model.event.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.supplier.model.DeleteRowsRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.UpdateRowsRawEventData;
import com.booking.replication.supplier.model.WriteRowsRawEventData;

public class DataAugmenter {
    private final AugmenterContext context;

    public DataAugmenter(AugmenterContext context) {
        this.context = context;
    }

    public AugmentedEventData apply(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        switch (eventHeader.getEventType()) {
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                WriteRowsRawEventData writeRowsRawEventData = WriteRowsRawEventData.class.cast(eventData);



                return new WriteRowsAugmentedEventData(
                        this.context.getEventTable(writeRowsRawEventData.getTableId()),
                        this.context.getIncludedColumns(writeRowsRawEventData.getIncludedColumns()),
                        this.context.getColumns(writeRowsRawEventData.getTableId()),
                        this.context.getRows(
                                writeRowsRawEventData.getTableId(),
                                writeRowsRawEventData.getIncludedColumns(),
                                writeRowsRawEventData.getRows()
                        ),
                        this.context.getAugmentedRows(
                                "INSERT",
                                this.context.getTransaction().getTimestamp(), // TODO: override these timestamps on transaction commit
                                this.context.getTransaction().getIdentifier().get(),
                                this.context.getTransaction().getXxid(),
                                writeRowsRawEventData.getTableId(),
                                writeRowsRawEventData.getIncludedColumns(),
                                writeRowsRawEventData.getRows()
                        )
                );
            // TODO: add augmentedRows to update augmented event data
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                UpdateRowsRawEventData updateRowsRawEventData = UpdateRowsRawEventData.class.cast(eventData);

                return new UpdateRowsAugmentedEventData(
                        this.context.getEventTable(updateRowsRawEventData.getTableId()),
                        this.context.getIncludedColumns(updateRowsRawEventData.getIncludedColumnsBeforeUpdate()),
                        this.context.getIncludedColumns(updateRowsRawEventData.getIncludedColumns()),
                        this.context.getColumns(updateRowsRawEventData.getTableId()),
                        this.context.getUpdatedRows(
                                updateRowsRawEventData.getTableId(),
                                updateRowsRawEventData.getIncludedColumns(),
                                updateRowsRawEventData.getRows()
                        )
                );

            // TODO: add augmentedRows to delete augmented event data
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                DeleteRowsRawEventData deleteRowsRawEventData = DeleteRowsRawEventData.class.cast(eventData);

                return new DeleteRowsAugmentedEventData(
                        this.context.getEventTable(deleteRowsRawEventData.getTableId()),
                        this.context.getIncludedColumns(deleteRowsRawEventData.getIncludedColumns()),
                        this.context.getColumns(deleteRowsRawEventData.getTableId()),
                        this.context.getRows(
                                deleteRowsRawEventData.getTableId(),
                                deleteRowsRawEventData.getIncludedColumns(),
                                deleteRowsRawEventData.getRows()
                        )
                );
            case QUERY:
                QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);

                return new QueryAugmentedEventData(
                        this.context.getQueryType(),
                        this.context.getQueryOperationType(),
                        this.context.getEventTable(),
                        queryRawEventData.getThreadId(),
                        queryRawEventData.getExecutionTime(),
                        queryRawEventData.getErrorCode(),
                        queryRawEventData.getSQL(),
                        this.context.getSchemaBefore(),
                        this.context.getSchemaAfter()
                );
            case XID:
                return new QueryAugmentedEventData(
                        this.context.getQueryType(),
                        this.context.getQueryOperationType(),
                        this.context.getEventTable(),
                        0L,
                        0L,
                        0,
                        null,
                        this.context.getSchemaBefore(),
                        this.context.getSchemaAfter()
                );
            default:
                return null;
        }
    }
}
