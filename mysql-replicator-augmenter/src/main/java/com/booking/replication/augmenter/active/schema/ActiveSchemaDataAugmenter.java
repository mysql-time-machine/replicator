package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventData;
import com.booking.replication.augmenter.model.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.QueryAugmentedEventData;
import com.booking.replication.augmenter.model.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.WriteRowsAugmentedEventData;
import com.booking.replication.supplier.model.DeleteRowsRawEventData;
import com.booking.replication.supplier.model.QueryRawEventData;
import com.booking.replication.supplier.model.RawEvent;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;
import com.booking.replication.supplier.model.UpdateRowsRawEventData;
import com.booking.replication.supplier.model.WriteRowsRawEventData;

import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveSchemaDataAugmenter  implements Function<RawEvent, AugmentedEventData> {
    private static final Logger LOG = Logger.getLogger(ActiveSchemaDataAugmenter.class.getName());

    private final ActiveSchemaContext context;

    public ActiveSchemaDataAugmenter(ActiveSchemaContext context) {
        this.context = context;
    }

    @Override
    public AugmentedEventData apply(RawEvent rawEvent) {
        RawEventHeaderV4 eventHeader = rawEvent.getHeader();
        RawEventData eventData = rawEvent.getData();

        if (this.context.getTransaction().started()) {
            if (!this.context.getTransaction().add(this.getAugmentedEventData(eventHeader, eventData))) {
                ActiveSchemaDataAugmenter.LOG.log(Level.WARNING, "cannot add to transaction");
            }

            return null;
        } else if (this.context.getTransaction().committed()) {
            if (!this.context.getTransaction().add(this.getAugmentedEventData(eventHeader, eventData))) {
                ActiveSchemaDataAugmenter.LOG.log(Level.WARNING, "cannot add to transaction");
            }

            return this.context.getTransaction().getData();
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
                        writeRowsRawEventData.getIncludedColumns(),
                        writeRowsRawEventData.getRows()
                );
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                UpdateRowsRawEventData updateRowsRawEventData = UpdateRowsRawEventData.class.cast(eventData);

                return new UpdateRowsAugmentedEventData(
                        updateRowsRawEventData.getIncludedColumnsBeforeUpdate(),
                        updateRowsRawEventData.getIncludedColumns(),
                        updateRowsRawEventData.getRows()
                );
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                DeleteRowsRawEventData deleteRowsRawEventData = DeleteRowsRawEventData.class.cast(eventData);

                return new DeleteRowsAugmentedEventData(
                        deleteRowsRawEventData.getIncludedColumns(),
                        deleteRowsRawEventData.getRows()
                );
            case QUERY:
                QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);

                return new QueryAugmentedEventData(
                        this.context.getQueryType(),
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
}
