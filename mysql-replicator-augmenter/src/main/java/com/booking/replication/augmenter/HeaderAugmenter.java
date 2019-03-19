package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.event.AugmentedEventHeader;
import com.booking.replication.augmenter.model.event.AugmentedEventType;
import com.booking.replication.augmenter.model.schema.FullTableName;
import com.booking.replication.supplier.model.RawEventData;
import com.booking.replication.supplier.model.RawEventHeaderV4;

public class HeaderAugmenter {
    private final AugmenterContext context;

    public HeaderAugmenter(AugmenterContext context) {
        this.context = context;
    }

    public synchronized AugmentedEventHeader apply(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        AugmentedEventType type = this.getAugmentedEventType(eventHeader);

        if (type == null) {
            return null;
        }
        FullTableName eventTable = this.context.getEventTable();
        String dbName = null;
        String tableName = null;
        if (eventTable != null) {
            dbName = eventTable.getDatabase();
            tableName = eventTable.getName();
        }
        return new AugmentedEventHeader(eventHeader.getTimestamp(), this.context.newCheckpoint(), type, dbName, tableName);
    }

    private AugmentedEventType getAugmentedEventType(RawEventHeaderV4 eventHeader) {
        switch (eventHeader.getEventType()) {
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                return AugmentedEventType.WRITE_ROWS;
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                return AugmentedEventType.UPDATE_ROWS;
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                return AugmentedEventType.DELETE_ROWS;
            case QUERY:
            case XID:
                return AugmentedEventType.QUERY;
            default:
                return null;
        }
    }
}
