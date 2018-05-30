package com.booking.replication.augmenter.active.schema;

import com.booking.replication.augmenter.model.AugmentedEventHeader;
import com.booking.replication.augmenter.model.AugmentedEventType;
import com.booking.replication.commons.checkpoint.Checkpoint;
import com.booking.replication.supplier.model.*;

import java.util.Map;
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
    }

    @Override
    public AugmentedEventHeader apply(RawEvent rawEvent) {
        RawEventHeaderV4 eventHeader = rawEvent.getHeader();

        this.serverId.set(eventHeader.getServerId());

        if (eventHeader.getRawEventType() == RawEventType.ROTATE) {
            RotateRawEventData eventData = RotateRawEventData.class.cast(rawEvent.getData());

            this.binlogFilename.set(eventData.getBinlogFilename());
            this.binlogPosition.set(eventData.getBinlogPosition());
        } else if (eventHeader.getRawEventType() == RawEventType.QUERY) {
            QueryRawEventData eventData = QueryRawEventData.class.cast(rawEvent.getData());
            Matcher matcher = this.pseudoGTIDPattern.matcher(eventData.getSQL());

            if (matcher.find() && matcher.groupCount() == 1) {
                this.pseudoGTID.set(matcher.group(0));
                this.pseudoGTIDIndex.set(0);
            }
        }

        try {
            return new AugmentedEventHeader(
                    eventHeader.getTimestamp(),
                    new Checkpoint(
                            this.serverId.get(),
                            this.binlogFilename.get(),
                            this.binlogPosition.get(),
                            this.pseudoGTID.get(),
                            this.pseudoGTIDIndex.get()
                    ),
                    this.getAugmentedEventType(rawEvent),
                    this.getTableName(rawEvent)
            );
        } finally {
            this.pseudoGTIDIndex.getAndIncrement();
        }
    }

    private AugmentedEventType getAugmentedEventType(RawEvent rawEvent) {
        return null;
    }

    private String getTableName(RawEvent rawEvent) {
        return null;
    }
}
