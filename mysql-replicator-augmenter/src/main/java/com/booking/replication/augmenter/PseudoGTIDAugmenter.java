package com.booking.replication.augmenter;

import com.booking.replication.model.*;
import com.booking.replication.model.augmented.AugmentedEventHeader;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PseudoGTIDAugmenter implements Augmenter {
    private class PseudoGTIDEventData implements AugmentedEventHeader {
        private EventHeaderV4 eventHeader;
        private String pseudoGTID;
        private int pseudoGTIDIndex;

        private PseudoGTIDEventData(EventHeaderV4 eventHeader, String pseudoGTID, int pseudoGTIDIndex) {
            this.eventHeader = eventHeader;
            this.pseudoGTID = pseudoGTID;
            this.pseudoGTIDIndex = pseudoGTIDIndex;
        }

        @Override
        public long getServerId() {
            return this.eventHeader.getServerId();
        }

        @Override
        public long getEventLength() {
            return this.eventHeader.getEventLength();
        }

        @Override
        public long getNextPosition() {
            return this.eventHeader.getNextPosition();
        }

        @Override
        public int getFlags() {
            return this.eventHeader.getFlags();
        }

        @Override
        public long getTimestamp() {
            return this.eventHeader.getTimestamp();
        }

        @Override
        public EventType getEventType() {
            return this.eventHeader.getEventType();
        }

        @Override
        public String getPseudoGTID() {
            return this.pseudoGTID;
        }

        @Override
        public int getPseudoGTIDIndex() {
            return this.pseudoGTIDIndex;
        }
    }

    private static final String DEFAULT_PSEUDO_GTID_PATTERN = "(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})";

    private final Pattern pseudoGTIDPattern;
    private final AtomicReference<String> currentPseudoGTID;
    private final AtomicInteger currentIndex;

    public PseudoGTIDAugmenter(Map<String, String> configuration) {
        this.pseudoGTIDPattern = Pattern.compile(
                configuration.getOrDefault(
                        Configuration.PSEUDO_GTID_PATTERN,
                        PseudoGTIDAugmenter.DEFAULT_PSEUDO_GTID_PATTERN
                ),
                Pattern.CASE_INSENSITIVE
        );
        this.currentPseudoGTID = new AtomicReference<>();
        this.currentIndex = new AtomicInteger();
    }

    @Override
    public Event apply(Event event) {
        if (event.getHeader().getEventType() == EventType.QUERY) {
            QueryEventData eventData = QueryEventData.class.cast(event.getData());
            Matcher matcher = this.pseudoGTIDPattern.matcher(eventData.getSQL());

            if (matcher.find() && matcher.groupCount() == 1) {
                this.currentPseudoGTID.set(matcher.group(0));
                this.currentIndex.set(0);
            } else {
                this.currentIndex.incrementAndGet();
            }
        } else {
            this.currentIndex.incrementAndGet();
        }

        return new EventImplementation<>(
                new PseudoGTIDEventData(
                        event.getHeader(),
                        this.currentPseudoGTID.get(),
                        this.currentIndex.get()
                ),
                event.getData()
        );
    }
}
