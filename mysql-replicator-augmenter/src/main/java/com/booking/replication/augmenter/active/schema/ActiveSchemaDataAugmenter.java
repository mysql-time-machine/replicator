package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.QueryAugmentedEventData;
import com.booking.replication.augmenter.model.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.WriteRowsAugmentedEventData;
import com.booking.replication.supplier.model.DeleteRowsRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.UpdateRowsRawEventData;
import com.booking.replication.supplier.model.WriteRowsRawEventData;

public class ActiveSchemaDataAugmenter {
    private final ActiveSchemaContext context;

    public ActiveSchemaDataAugmenter(ActiveSchemaContext context) {
        this.context = context;
    }

    public AugmentedEventData apply(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        switch (eventHeader.getEventType()) {
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                WriteRowsRawEventData writeRowsRawEventData = WriteRowsRawEventData.class.cast(eventData);

                return new WriteRowsAugmentedEventData(
                        this.context.getEventTable(writeRowsRawEventData.getTableId()),
                        this.context.getColumns(writeRowsRawEventData.getTableId(), writeRowsRawEventData.getIncludedColumns()),
                        writeRowsRawEventData.getRows()
                );
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                UpdateRowsRawEventData updateRowsRawEventData = UpdateRowsRawEventData.class.cast(eventData);

                return new UpdateRowsAugmentedEventData(
                        this.context.getEventTable(updateRowsRawEventData.getTableId()),
                        this.context.getColumns(updateRowsRawEventData.getTableId(), updateRowsRawEventData.getIncludedColumnsBeforeUpdate()),
                        this.context.getColumns(updateRowsRawEventData.getTableId(), updateRowsRawEventData.getIncludedColumns()),
                        this.context.getUpdatedRows(updateRowsRawEventData.getRows())
                );
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                DeleteRowsRawEventData deleteRowsRawEventData = DeleteRowsRawEventData.class.cast(eventData);

                return new DeleteRowsAugmentedEventData(
                        this.context.getEventTable(deleteRowsRawEventData.getTableId()),
                        this.context.getColumns(deleteRowsRawEventData.getTableId(), deleteRowsRawEventData.getIncludedColumns()),
                        deleteRowsRawEventData.getRows()
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
                        this.context.getCreateTableBefore(),
                        this.context.getCreateTableAfter()
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
                        this.context.getCreateTableBefore(),
                        this.context.getCreateTableAfter()
                );
            default:
                return null;
        }
    }
}
