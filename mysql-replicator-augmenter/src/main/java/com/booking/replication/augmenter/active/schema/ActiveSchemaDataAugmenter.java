package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventColumn;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.QueryAugmentedEventData;
import com.booking.replication.augmenter.model.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.WriteRowsAugmentedEventData;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.supplier.model.DeleteRowsRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.UpdateRowsRawEventData;
import com.booking.replication.supplier.model.WriteRowsRawEventData;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveSchemaDataAugmenter {
    private static final Logger LOG = Logger.getLogger(ActiveSchemaDataAugmenter.class.getName());

    private final ActiveSchemaContext context;
    private final ActiveSchemaManager manager;

    public ActiveSchemaDataAugmenter(ActiveSchemaContext context, ActiveSchemaManager manager) {
        this.context = context;
        this.manager = manager;
    }

    public AugmentedEventData apply(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        if (this.context.getTransaction().started()) {
            if (!this.context.getTransaction().add(this.getAugmentedEventData(eventHeader, eventData))) {
                ActiveSchemaDataAugmenter.LOG.log(Level.WARNING, "cannot add to transaction");
            }

            return null;
        } else if (this.context.getTransaction().committed()) {
            if (!this.context.getTransaction().add(this.getAugmentedEventData(eventHeader, eventData))) {
                ActiveSchemaDataAugmenter.LOG.log(Level.WARNING, "cannot add to transaction");
            }

            if (this.context.getTransaction().overloaded()) {
                this.context.getTransaction().clean();

                throw new ForceRewindException("transaction overloaded");
            }

            return this.context.getTransaction().clean();
        } else {
            return this.getAugmentedEventData(eventHeader, eventData);
        }
    }

    private AugmentedEventData getAugmentedEventData(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        switch (eventHeader.getEventType()) {
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                WriteRowsRawEventData writeRowsRawEventData = WriteRowsRawEventData.class.cast(eventData);

                return new WriteRowsAugmentedEventData(
                        this.getColumns(writeRowsRawEventData.getTableId(), writeRowsRawEventData.getIncludedColumns()),
                        writeRowsRawEventData.getRows()
                );
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                UpdateRowsRawEventData updateRowsRawEventData = UpdateRowsRawEventData.class.cast(eventData);

                return new UpdateRowsAugmentedEventData(
                        this.getColumns(updateRowsRawEventData.getTableId(), updateRowsRawEventData.getIncludedColumnsBeforeUpdate()),
                        this.getColumns(updateRowsRawEventData.getTableId(), updateRowsRawEventData.getIncludedColumns()),
                        updateRowsRawEventData.getRows()
                );
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                DeleteRowsRawEventData deleteRowsRawEventData = DeleteRowsRawEventData.class.cast(eventData);

                return new DeleteRowsAugmentedEventData(
                        this.getColumns(deleteRowsRawEventData.getTableId(), deleteRowsRawEventData.getIncludedColumns()),
                        deleteRowsRawEventData.getRows()
                );
            case QUERY:
                QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);

                return new QueryAugmentedEventData(
                        this.context.getQueryType(),
                        this.context.getQueryOperationType(),
                        queryRawEventData.getThreadId(),
                        queryRawEventData.getExecutionTime(),
                        queryRawEventData.getErrorCode(),
                        queryRawEventData.getDatabase(),
                        queryRawEventData.getSQL()
                );
            default:
                return null;
        }
    }

    private List<AugmentedEventColumn> getColumns(long tableId, BitSet includedColumns) {
        List<AugmentedEventColumn> columnList = this.manager.listColumns(this.context.getTable(tableId).getName());
        List<AugmentedEventColumn> includedColumnList = new LinkedList<>();

        for (int columnIndex = 0; columnIndex < columnList.size(); columnIndex++) {
            if (includedColumns.get(columnIndex)) {
                includedColumnList.add(columnList.get(columnIndex));
            }
        }

        return includedColumnList;
    }
}
