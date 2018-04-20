package com.booking.replication.augmenter;

import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.model.EventHeaderV4;
import com.booking.replication.model.EventImplementation;
import com.booking.replication.model.EventType;
import com.booking.replication.model.QueryEventData;
import com.booking.replication.model.RotateEventData;
import com.booking.replication.augmenter.active.schema.augmented.AugmentedEventHeaderImplementation;

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
    public Event apply(Event event) {
        EventHeaderV4 eventHeader = event.getHeader();

        this.serverId.set(eventHeader.getServerId());

        if (eventHeader.getEventType() == EventType.ROTATE) {
            RotateEventData eventData = RotateEventData.class.cast(event.getData());

            this.binlogFilename.set(eventData.getBinlogFilename());
            this.binlogPosition.set(eventData.getBinlogPosition());
        } else if (eventHeader.getEventType() == EventType.QUERY) {
            QueryEventData eventData = QueryEventData.class.cast(event.getData());
            Matcher matcher = this.pseudoGTIDPattern.matcher(eventData.getSQL());

            if (matcher.find() && matcher.groupCount() == 1) {
                this.pseudoGTID.set(matcher.group(0));
                this.pseudoGTIDIndex.set(0);
            }
        }

        try {
            return new EventImplementation<>(
                    new AugmentedEventHeaderImplementation(
                            eventHeader,
                            new Checkpoint(
                                    this.serverId.get(),
                                    this.binlogFilename.get(),
                                    this.binlogPosition.get(),
                                    this.pseudoGTID.get(),
                                    this.pseudoGTIDIndex.get()
                            )
                    ),
                    event.getData()
            );
        } finally {
            this.pseudoGTIDIndex.getAndIncrement();
        }
    }
}
