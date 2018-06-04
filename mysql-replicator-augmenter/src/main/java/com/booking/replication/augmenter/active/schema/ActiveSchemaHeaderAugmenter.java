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

public class ActiveSchemaHeaderAugmenter implements Function<RawEvent, AugmentedEventHeader> {
    private final ActiveSchemaContext context;

    public ActiveSchemaHeaderAugmenter(ActiveSchemaContext context) {
        this.context = context;
    }

    @Override
    public AugmentedEventHeader apply(RawEvent rawEvent) {
        RawEventHeaderV4 eventHeader = rawEvent.getHeader();
        RawEventData eventData = rawEvent.getData();

        this.context.updateContext(eventHeader, eventData);

        if (this.context.hasData()) {
            AugmentedEventType type = this.getAugmentedEventType(eventHeader);

            if (type == null) {
                return null;
            }

            return new AugmentedEventHeader(
                    rawEvent.getHeader().getTimestamp(),
                    this.context.getCheckpoint(),
                    type,
                    this.getAugmentedEventTable(eventHeader, eventData)
            );
        } else {
            return null;
        }
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
                return AugmentedEventType.QUERY;
            default:
                return null;
        }
    }

    private AugmentedEventTable getAugmentedEventTable(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        switch (eventHeader.getEventType()) {
            case WRITE_ROWS:
            case UPDATE_ROWS:
            case DELETE_ROWS:
                return this.context.getTableName(TableIdRawEventData.class.cast(eventData).getTableId());
            default:
                return null;
        }
    }
}
