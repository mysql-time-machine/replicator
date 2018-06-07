package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.augmenter.model.AugmentedEventTable;
import com.booking.replication.augmenter.model.AugmentedEventType;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ActiveSchemaHeaderAugmenter {
    private final ActiveSchemaContext context;

    public ActiveSchemaHeaderAugmenter(ActiveSchemaContext context) {
        this.context = context;
    }

    public AugmentedEventHeader apply(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        AugmentedEventType type = this.getAugmentedEventType(eventHeader);

        if (type == null) {
            return null;
        }

        long timestamp = this.context.getTransaction().committed()?this.context.getTransaction().getTimestamp():eventHeader.getTimestamp();

        Checkpoint checkpoint = this.context.getCheckpoint();
        AugmentedEventTable eventTable = this.getAugmentedEventTable(eventHeader, eventData);

        return new AugmentedEventHeader(timestamp, checkpoint, type, eventTable);
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
                if (this.context.getTransaction().committed()) {
                    return AugmentedEventType.TRANSACTION;
                } else {
                    return AugmentedEventType.QUERY;
                }
            default:
                return null;
        }
    }

    private AugmentedEventTable getAugmentedEventTable(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        switch (eventHeader.getEventType()) {
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                return this.context.getTable(TableIdRawEventData.class.cast(eventData).getTableId());
            case QUERY:
            case XID:
                return this.context.getTable();
            default:
                return null;
        }
    }
}
