package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.augmenter.model.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.QueryAugmentedEventData;
import com.booking.replication.augmenter.model.TransactionAugmentedEventData;
import com.booking.replication.augmenter.model.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.WriteRowsAugmentedEventData;
import com.booking.replication.commons.checkpoint.ForceRewindException;
import com.booking.replication.supplier.model.DeleteRowsRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.UpdateRowsRawEventData;
import com.booking.replication.supplier.model.WriteRowsRawEventData;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveSchemaDataAugmenter {
    private static final Logger LOG = Logger.getLogger(ActiveSchemaDataAugmenter.class.getName());

    private final ActiveSchemaContext context;

    public ActiveSchemaDataAugmenter(ActiveSchemaContext context) {
        this.context = context;
    }

    public AugmentedEventData apply(RawEventHeaderV4 eventHeader, RawEventData eventData, AugmentedEventHeader augmentedEventHeader) {
        if (this.context.getTransaction().committed()) {
            if (this.context.getTransaction().sizeLimitExceeded()) {
                this.context.getTransaction().clean();

                throw new ForceRewindException("transaction size limit exceeded");
            } else {
                return this.context.getTransaction().clean();
            }
        } else if (this.context.getTransaction().started()) {
            if (this.context.getTransaction().resuming() && this.context.getTransaction().sizeLimitExceeded()) {
                TransactionAugmentedEventData augmentedEventData =  this.context.getTransaction().clean();

                augmentedEventData.getEventList().add(new AugmentedEvent(augmentedEventHeader, this.getAugmentedEventData(eventHeader, eventData)));

                return augmentedEventData;
            } else if (!this.context.getTransaction().add(new AugmentedEvent(augmentedEventHeader, this.getAugmentedEventData(eventHeader, eventData)))) {
                ActiveSchemaDataAugmenter.LOG.log(Level.WARNING, "cannot add to transaction");
            }

            return null;
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
                        this.context.getColumns(writeRowsRawEventData.getTableId(), writeRowsRawEventData.getIncludedColumns()),
                        writeRowsRawEventData.getRows()
                );
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                UpdateRowsRawEventData updateRowsRawEventData = UpdateRowsRawEventData.class.cast(eventData);

                return new UpdateRowsAugmentedEventData(
                        this.context.getColumns(updateRowsRawEventData.getTableId(), updateRowsRawEventData.getIncludedColumnsBeforeUpdate()),
                        this.context.getColumns(updateRowsRawEventData.getTableId(), updateRowsRawEventData.getIncludedColumns()),
                        updateRowsRawEventData.getRows()
                );
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                DeleteRowsRawEventData deleteRowsRawEventData = DeleteRowsRawEventData.class.cast(eventData);

                return new DeleteRowsAugmentedEventData(
                        this.context.getColumns(deleteRowsRawEventData.getTableId(), deleteRowsRawEventData.getIncludedColumns()),
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
                        queryRawEventData.getSQL(),
                        null,
                        null
                );
            default:
                return null;
        }
    }
}
