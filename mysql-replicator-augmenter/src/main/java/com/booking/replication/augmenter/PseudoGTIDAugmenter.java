package com.booking.replication.augmenter;

import com.booking.replication.augmenter.model.AugmentedEvent;
import com.booking.replication.augmenter.model.AugmentedEventImplementation;
import com.booking.replication.augmenter.model.PseudoGTIDEventHeaderImplementation;
import com.booking.replication.supplier.model.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PseudoGTIDAugmenter implements Augmenter {

    private static final String DEFAULT_PSEUDO_GTID_PATTERN = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";

    private final Pattern pseudoGTIDPattern;
    private final AtomicLong serverId;
    private final AtomicReference<String> binlogFilename;
    private final AtomicLong binlogPosition;
    private final AtomicReference<String> pseudoGTID;
    private final AtomicInteger pseudoGTIDIndex;

    public PseudoGTIDAugmenter(Map<String, String> configuration) {
        this.pseudoGTIDPattern = Pattern.compile(
                configuration.getOrDefault(
                        Configuration.PSEUDO_GTID_PATTERN,
                        PseudoGTIDAugmenter.DEFAULT_PSEUDO_GTID_PATTERN
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
    public AugmentedEvent apply(RawEvent rawEvent) {
        EventHeaderV4 eventHeader = rawEvent.getHeader();

        this.serverId.set(eventHeader.getServerId());

        if (eventHeader.getRawEventType() == RawEventType.ROTATE) {
            RotateEventData eventData = RotateEventData.class.cast(rawEvent.getData());

            this.binlogFilename.set(eventData.getBinlogFilename());
            this.binlogPosition.set(eventData.getBinlogPosition());
        } else if (eventHeader.getRawEventType() == RawEventType.QUERY) {
            QueryEventData eventData = QueryEventData.class.cast(rawEvent.getData());
            Matcher matcher = this.pseudoGTIDPattern.matcher(eventData.getSQL());

            if (matcher.find() && matcher.groupCount() == 1) {
                this.pseudoGTID.set(matcher.group(0));
                this.pseudoGTIDIndex.set(0);
            }
        }

        try {
//            return new AugmentedEventImplementation(
//                    new PseudoGTIDEventHeaderImplementation(
//                            eventHeader,
//                            new Checkpoint(
//                                    this.serverId.get(),
//                                    this.binlogFilename.get(),
//                                    this.binlogPosition.get(),
//                                    this.pseudoGTID.get(),
//                                    this.pseudoGTIDIndex.get()
//                            )
//                    ),
//                    rawEvent.getData()
//            );
            return null;
        } finally {
            this.pseudoGTIDIndex.getAndIncrement();
        }
    }
}
