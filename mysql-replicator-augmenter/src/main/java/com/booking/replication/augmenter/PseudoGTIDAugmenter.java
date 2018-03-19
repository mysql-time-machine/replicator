package com.booking.replication.augmenter;

import com.booking.replication.model.Checkpoint;
import com.booking.replication.model.Event;
import com.booking.replication.model.EventHeaderV4;
import com.booking.replication.model.EventImplementation;
import com.booking.replication.model.EventType;
import com.booking.replication.model.QueryEventData;
import com.booking.replication.model.RotateEventData;
import com.booking.replication.model.augmented.AugmentedEventHeaderImplementation;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PseudoGTIDAugmenter implements Augmenter {

    private static final String DEFAULT_PSEUDO_GTID_PATTERN = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";

    private final Pattern pseudoGTIDPattern;
    private final Checkpoint checkpoint;

    public PseudoGTIDAugmenter(Map<String, String> configuration) {
        this.pseudoGTIDPattern = Pattern.compile(
                configuration.getOrDefault(
                        Configuration.PSEUDO_GTID_PATTERN,
                        PseudoGTIDAugmenter.DEFAULT_PSEUDO_GTID_PATTERN
                ),
                Pattern.CASE_INSENSITIVE
        );
        this.checkpoint = new Checkpoint();
    }

    @Override
    public Event apply(Event event) {
        EventHeaderV4 eventHeader = event.getHeader();

        this.checkpoint.setServerId(eventHeader.getServerId());

        if (eventHeader.getEventType() == EventType.ROTATE) {
            RotateEventData eventData = RotateEventData.class.cast(event.getData());

            this.checkpoint.setBinlogFilename(eventData.getBinlogFilename());
            this.checkpoint.setBinlogPosition(eventData.getBinlogPosition());
        } else if (eventHeader.getEventType() == EventType.QUERY) {
            QueryEventData eventData = QueryEventData.class.cast(event.getData());
            Matcher matcher = this.pseudoGTIDPattern.matcher(eventData.getSQL());

            if (matcher.find() && matcher.groupCount() == 1) {
                this.checkpoint.setPseudoGTID(matcher.group(0));
                this.checkpoint.setPseudoGTIDIndex(0);
            }
        }

        try {
            return new EventImplementation<>(
                    new AugmentedEventHeaderImplementation(
                            eventHeader,
                            new Checkpoint(this.checkpoint)
                    ),
                    event.getData()
            );
        } finally {
            this.checkpoint.setPseudoGTIDIndex(this.checkpoint.getPseudoGTIDIndex() + 1);
        }
    }
}
