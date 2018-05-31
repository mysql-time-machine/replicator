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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ActiveSchemaHeaderAugmenter implements Function<RawEvent, AugmentedEventHeader> {
    public interface Configuration extends ActiveSchemaAugmenter.Configuration {
        String PSEUDO_GTID_PATTERN = "augmenter.pseudogtid.pattern";
    }

    private static final String DEFAULT_PSEUDO_GTID_PATTERN = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";

    private final Pattern pseudoGTIDPattern;
    private final AtomicLong serverId;
    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;
    private final AtomicReference<String> pseudoGTID;
    private final AtomicInteger pseudoGTIDIndex;
    private final Map<Long, AugmentedEventTable> tableIdTableMap;

    public ActiveSchemaHeaderAugmenter(Map<String, String> configuration) {
        this.pseudoGTIDPattern = Pattern.compile(
                configuration.getOrDefault(
                        Configuration.PSEUDO_GTID_PATTERN,
                        ActiveSchemaHeaderAugmenter.DEFAULT_PSEUDO_GTID_PATTERN
                ),
                Pattern.CASE_INSENSITIVE
        );

        this.serverId = new AtomicLong();
        this.binlogFilename = new AtomicReference<>();
        this.binlogPosition = new AtomicLong();
        this.pseudoGTID = new AtomicReference<>();
        this.pseudoGTIDIndex = new AtomicInteger();
        this.tableIdTableMap = new ConcurrentHashMap<>();
    }

    @Override
    public AugmentedEventHeader apply(RawEvent rawEvent) {
        RawEventHeaderV4 eventHeader = rawEvent.getHeader();
        RawEventData eventData = rawEvent.getData();

        this.updateContext(eventHeader, eventData);

        try {
            return new AugmentedEventHeader(
                    rawEvent.getHeader().getTimestamp(),
                    new Checkpoint(
                            this.serverId.get(),
                            this.binlogFilename.get(),
                            this.binlogPosition.get(),
                            this.pseudoGTID.get(),
                            this.pseudoGTIDIndex.get()
                    ),
                    this.getAugmentedEventType(eventHeader, eventData),
                    this.getAugmentedEventTable(eventHeader, eventData)
            );
        } finally {
            this.pseudoGTIDIndex.getAndIncrement();
        }
    }

    private void updateContext(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        this.serverId.set(eventHeader.getServerId());

        switch (eventHeader.getEventType()) {
            case ROTATE:
                RotateRawEventData rotateRawEventData = RotateRawEventData.class.cast(eventData);

                this.binlogFilename.set(rotateRawEventData.getBinlogFilename());
                this.binlogPosition.set(rotateRawEventData.getBinlogPosition());

                break;
            case QUERY:
                QueryRawEventData queryRawEventData = QueryRawEventData.class.cast(eventData);
                Matcher matcher = this.pseudoGTIDPattern.matcher(queryRawEventData.getSQL());

                if (matcher.find() && matcher.groupCount() == 1) {
                    this.pseudoGTID.set(matcher.group(0));
                    this.pseudoGTIDIndex.set(0);
                }

                break;
            case TABLE_MAP:
                TableMapRawEventData tableMapRawEventData = TableMapRawEventData.class.cast(eventData);

                this.tableIdTableMap.put(
                        tableMapRawEventData.getTableId(),
                        new AugmentedEventTable(
                                tableMapRawEventData.getDatabase(),
                                tableMapRawEventData.getTable()
                        )
                );

                break;
        }
    }

    private AugmentedEventType getAugmentedEventType(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        return null;
    }

    private AugmentedEventTable getAugmentedEventTable(RawEventHeaderV4 eventHeader, RawEventData eventData) {
        switch (eventHeader.getEventType()) {
            case WRITE_ROWS:
            case UPDATE_ROWS:
            case DELETE_ROWS:
                return this.tableIdTableMap.get(TableIdRawEventData.class.cast(eventData).getTableId());
            default:
                return null;
        }
    }
}
