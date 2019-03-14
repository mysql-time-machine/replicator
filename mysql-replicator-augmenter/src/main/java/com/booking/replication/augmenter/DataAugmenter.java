package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.event.AugmentedEventData;
import com.booking.replication.augmenter.model.event.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.QueryAugmentedEventData;
import com.booking.replication.augmenter.model.event.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.row.RowBeforeAfter;
import com.booking.replication.augmenter.model.schema.ColumnSchema;
import com.booking.replication.supplier.model.DeleteRowsRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.UpdateRowsRawEventData;
import com.booking.replication.supplier.model.WriteRowsRawEventData;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class DataAugmenter {

    private final AugmenterContext context;

    public DataAugmenter(AugmenterContext context) {
        this.context = context;
    }

    public synchronized AugmentedEventData apply(RawEventHeaderV4 eventHeader, RawEventData eventData) {

        switch (eventHeader.getEventType()) {

            case WRITE_ROWS:
            case EXT_WRITE_ROWS:

                WriteRowsRawEventData writeRowsRawEventData = WriteRowsRawEventData.class.cast(eventData);

                final BitSet includedColumnsInsert = writeRowsRawEventData.getIncludedColumns();

                List<RowBeforeAfter> eventRowsBeforeAfter = writeRowsRawEventData
                        .getRows()
                        .stream()
                        .map(
                                r -> new RowBeforeAfter(includedColumnsInsert, null, r)
                            ).collect(Collectors.toList());

                this.context.incrementRowCounterMetrics(eventHeader.getEventType(), eventRowsBeforeAfter.size());

                Collection<ColumnSchema> columns = this.context.getColumns(writeRowsRawEventData.getTableId());
                if(columns == null) return null;
                return new WriteRowsAugmentedEventData(

                        this.context.getEventTable(writeRowsRawEventData.getTableId()),
                        this.context.getIncludedColumns(writeRowsRawEventData.getIncludedColumns()),
                        columns,

                        this.context.computeAugmentedEventRows(
                                "INSERT",
                                this.context.getTransaction().getIdentifier().get(),
                                this.context.getTransaction().getXxid(),
                                writeRowsRawEventData.getTableId(),
                                includedColumnsInsert,
                                eventRowsBeforeAfter
                        )
                );

            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                UpdateRowsRawEventData updateRowsRawEventData = UpdateRowsRawEventData.class.cast(eventData);

                final BitSet includedColumnsUpdate = updateRowsRawEventData.getIncludedColumns();

                List<RowBeforeAfter> rowsBeforeAfterUpdate = updateRowsRawEventData
                        .getRows()
                        .stream()
                        .map(
                                r -> new RowBeforeAfter(includedColumnsUpdate, r.getKey(), r.getValue())
                        ).collect(Collectors.toList());

                this.context.incrementRowCounterMetrics(eventHeader.getEventType(), rowsBeforeAfterUpdate.size());
                Collection<ColumnSchema> columns1 = this.context.getColumns(updateRowsRawEventData.getTableId());
                if(columns1 == null) return null;
                return new UpdateRowsAugmentedEventData(

                        this.context.getEventTable(updateRowsRawEventData.getTableId()),
                        this.context.getIncludedColumns(updateRowsRawEventData.getIncludedColumnsBeforeUpdate()),
                        this.context.getIncludedColumns(updateRowsRawEventData.getIncludedColumns()),
                        columns1,

                        this.context.computeAugmentedEventRows(
                                "UPDATE",
                                this.context.getTransaction().getIdentifier().get(),
                                this.context.getTransaction().getXxid(),
                                updateRowsRawEventData.getTableId(),

                                includedColumnsUpdate,
                                rowsBeforeAfterUpdate
                        )
                );

            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                DeleteRowsRawEventData deleteRowsRawEventData = DeleteRowsRawEventData.class.cast(eventData);

                final BitSet includedColumnsDelete = deleteRowsRawEventData.getIncludedColumns();

                List<RowBeforeAfter> rowsBeforeAfterDelete = deleteRowsRawEventData
                        .getRows()
                        .stream()
                        .map(
                                r -> new RowBeforeAfter(includedColumnsDelete, r, null)
                        ).collect(Collectors.toList());

                this.context.incrementRowCounterMetrics(eventHeader.getEventType(), rowsBeforeAfterDelete.size());

                Collection<ColumnSchema> columns2 = this.context.getColumns(deleteRowsRawEventData.getTableId());
                if(columns2 == null) return null;
                return new DeleteRowsAugmentedEventData(
                        this.context.getEventTable(deleteRowsRawEventData.getTableId()),
                        this.context.getIncludedColumns(deleteRowsRawEventData.getIncludedColumns()),
                        columns2,
                        this.context.computeAugmentedEventRows(
                                "DELETE",
                                this.context.getTransaction().getIdentifier().get(),
                                this.context.getTransaction().getXxid(),
                                deleteRowsRawEventData.getTableId(),

                                includedColumnsDelete,
                                rowsBeforeAfterDelete
                        )
                );
            case QUERY:
                QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);

                this.context.incrementRowCounterMetrics(eventHeader.getEventType(), 1);
                
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
