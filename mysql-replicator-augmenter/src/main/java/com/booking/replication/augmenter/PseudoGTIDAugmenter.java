package com.booking.replication.augmenter;

import com.booking.replication.model.Event;
import com.booking.replication.model.EventImplementation;
import com.booking.replication.model.EventType;
import com.booking.replication.model.QueryEventData;
import com.booking.replication.model.augmented.AugmentedEventHeaderImplementation;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PseudoGTIDAugmenter implements Augmenter {

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
                new AugmentedEventHeaderImplementation(
                        event.getHeader(),
                        this.currentPseudoGTID.get(),
                        this.currentIndex.get()
                ),
                event.getData()
        );
    }
}
